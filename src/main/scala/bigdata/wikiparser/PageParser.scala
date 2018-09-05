package bigdata.wikiparser

import java.io.ByteArrayInputStream

import bigdata.input.XMLInputFormat
import info.bliki.wiki.dump.{IArticleFilter, Siteinfo, WikiArticle, WikiXMLParser}
import info.bliki.wiki.filter.WikipediaParser
import info.bliki.wiki.model.WikiModel
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.xml.sax.SAXException

object PageParser {
  /**
    * A helper class that allows for a WikiArticle to be serialized and also pulled from the XML parser
    *
    * @param page The WikiArticle that is being wrapped
    */
  case class WrappedPage(var page: WikiArticle = new WikiArticle) {}

  /**
    * Represents a parsed Wikipedia page from the Wikipedia XML dump
    *
    * https://en.wikipedia.org/wiki/Wikipedia:Database_download
    * https://meta.wikimedia.org/wiki/Data_dump_torrents#enwiki
    *
    * @param title Title of the current page
    * @param text Text of the current page including markup
    * @param isCategory Is the page a category page, not perfectly accurate
    * @param isFile Is the page a file page, not perfectly accurate
    * @param isTemplate Is the page a template page, not perfectly accurate
    */
  case class Page(title: String, text: String, isCategory: Boolean , isFile: Boolean, isTemplate: Boolean)

  /**
    * Helper class for parsing wiki XML, parsed pages are set in wrappedPage
    *
    */
  class SetterArticleFilter(val wrappedPage: WrappedPage) extends IArticleFilter {
    @throws(classOf[SAXException])
    def process(page: WikiArticle, siteinfo: Siteinfo)  {
      wrappedPage.page = page
    }
  }

  /**
    * Represents a redirect from one wiki article title to another
    *
    * @param pageTitle Title of the current article
    * @param redirectTitle Title of the article being redirected to
    */
  case class Redirect(pageTitle: String, redirectTitle: String)

  /**
    * Reads a wiki dump xml file, returning a single row for each <page>...</page>
    * https://en.wikipedia.org/wiki/Wikipedia:Database_download
    * https://meta.wikimedia.org/wiki/Data_dump_torrents#enwiki
    */
  def readWikiPagesFromDump(sc: SparkContext, file: String) : RDD[(Long, String)] = {
    println("ReadWkiPages")
    val conf = new Configuration()
    conf.set(XMLInputFormat.START_TAG_KEY, "<page>")
    conf.set(XMLInputFormat.END_TAG_KEY, "</page>")
    //    conf.set("io.compression.codecs","org.apache.hadoop.io.compress.BZip2Codec")
    val rdd = sc.newAPIHadoopFile(file, classOf[XMLInputFormat], classOf[LongWritable], classOf[Text], conf)
    rdd.map{case (k,v) => (k.get(), new String(v.copyBytes()))}
  }

  /**
    * Parses a page from raw xml text. The page is intended as single single
    * <page>...</page> tag with no revisions
    * @param text Raw xml text
    * @return Parsed Page object
    */
  def parsePageFromRawText(text: String) : Page = {
    val wrappedPage = new WrappedPage
    //The parser occasionally exceptions out, we ignore these
    try {
      val parser = new WikiXMLParser(new ByteArrayInputStream(text.getBytes), new SetterArticleFilter(wrappedPage))
      parser.parse()
    } catch {
      case e: Exception =>
    }
    val page = wrappedPage.page
    //    println("Parsing page with title: " + page.getTitle)
    if (page.getText != null
      && page.getId != null && page.getRevisionId != null
      && page.getTimeStamp != null) {
      Page(page.getTitle, page.getText, page.isCategory, page.isFile, page.isTemplate)
    } else {
      Page("", "", isCategory=false, isFile=false, isTemplate=false)
    }
  }

  /**
    * Parses the raw page text produced by readWikiDump into Page objects
    */
  def parsePagesWithIndex(rdd: RDD[(Long, String)]): RDD[(Long, Page)] = {
    rdd.mapValues{ text => { parsePageFromRawText(text) }}
  }

  def parsePages(rdd: RDD[(Long, String)]): RDD[Page] = {
    rdd.map{ case (_, text) => parsePageFromRawText(text) }
  }

  /**
    * Parses redirects out of the Page objects
    */
  def parseRedirects(rdd: RDD[Page]): RDD[Redirect] = {
    rdd.map {
      page =>
        val redirect =
          if (page.text != null && !page.isCategory && !page.isFile && !page.isTemplate) {
            val r =  WikipediaParser.parseRedirect(page.text, new WikiModel("", ""))
            if (r == null) {
              None
            } else {
              Some(Redirect(page.title,r))
            }
          } else {
            None
          }
        redirect
    }.filter(_.isDefined).map(_.get)
  }

  /**
    * Parses internal article links from a Page object, filtering out links that aren't to articles
    */
  def mapPagesToInternalLinksWthIndex(rdd: RDD[Page]): RDD[(String, List[Link])] = {
    rdd.map { page => (page.title, LinksParser.parseLinksFromPageContentWithCount(page.text)) }
  }

  def mapPagesToInternalLinks(rdd: RDD[Page]): RDD[Link] = {
    rdd.flatMap { page => { LinksParser.parseLinksFromPageContent(page) }}
  }
}
