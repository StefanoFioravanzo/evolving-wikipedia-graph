package bigdata.wikiparser

import java.io.ByteArrayInputStream

import info.bliki.wiki.dump.{IArticleFilter, Siteinfo, WikiArticle, WikiXMLParser}
import info.bliki.wiki.filter.WikipediaParser
import info.bliki.wiki.model.WikiModel
import org.apache.commons.lang3.StringEscapeUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.htmlcleaner.HtmlCleaner
import org.xml.sax.SAXException

import bigdata.input.XMLInputFormat
import bigdata.input.WikipediaInputFormat

// TODO: Substitute all println with LOG
object Parser {

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
    * Represent a link from one wiki article to another
    *
    * @param pageTitle Title of the current article
    * @param linkTitle Title of the linked article
    * @param count The count of links with this title in the page
    */
  case class Link(pageTitle: String, linkTitle: String, count: Int)


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

  def readWikiRevisionsFromDump(sc: SparkContext, file: String) : RDD[(String, String)] = {
    println("ReadWikiRevisions")
    val rdd = sc.newAPIHadoopFile(file, classOf[WikipediaInputFormat], classOf[Text], classOf[Text], new Configuration())
    rdd.map { case (title, revision) => (new String(title.copyBytes()), new String(revision.copyBytes())) }
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
    * Simple version: returns the links (defines by the <a/> html tag
    * present in the page
    */
  def parseLinksFromPage(page: Page) : List[Link] = {
    if (page.text != null) {
      try {
        val html = WikiModel.toHtml(page.text)
        val cleaner = new HtmlCleaner
        val rootNode = cleaner.clean(html)
        val elements = rootNode.getElementsByName("a", true)
        val out = for (
          elem <- elements;
          classType = elem.getAttributeByName("class");
          title = elem.getAttributeByName("title")
          if (
            title != null
              && !title.startsWith("User:") && !title.startsWith("User talk:")
              && (classType == null || !classType.contains("external"))
            )
        ) yield {
          Link(page.title, StringEscapeUtils.unescapeHtml4(title), 1)
        }
        out.toList
      }  catch {
        case e: Exception => Nil
      }
    } else {
      Nil
    }
  }

  //TODO: Currently HTMLCleaner selects also some <a> tags that are not links (probably image description and they break w/ regex parsing.
  //TODO: In some pages there is an empty link, which then during regex matching it matches all spaces. Find out why it gets included in the first place.
  /**
    * Extract all links in a page with recurrence counts
    * First pass: parse html and retrieve <a/> tags
    * Second pass: regex each found link to search for non-link occurrences in text
    */
  def parseLinksFromPageWithCount(pageText: String, pageTitle: String) : List[Link] = {
    if (pageText != null) {
      try {
        val html = WikiModel.toHtml(pageText)
        val cleaner = new HtmlCleaner
        val rootNode = cleaner.clean(html)
        val elements = rootNode.getElementsByName("a", true)
        val out = for (
          elem <- elements;
          classType = elem.getAttributeByName("class");
          title = elem.getAttributeByName("title")
          if (
            title != null
              && !title.startsWith("User:") && !title.startsWith("User talk:")
              && (classType == null || !classType.contains("external"))
              && title.length() < 256  // max allowed title length
            )
        ) yield {
          StringEscapeUtils.unescapeHtml4(title)
        }
        val counts = out.groupBy(identity).mapValues(_.length)
        var links = (counts map {case(title:String, count:Int) => Link(pageTitle, title, count)}).toList


        // Now parse the text for other occurrences of the links (not linked)
        // Exclude already parsed links
        cleaner.getProperties.setPruneTags("a")
        val rootNodeNoLinks = cleaner.clean(html)
        val text = cleaner.getInnerHtml(rootNodeNoLinks)

        for (i <- links.indices) {
          val link = links(i)
          // search for occurrences of this link in the page text

          //TODO: Benchmark these approaches. Find best one. Empirically Regex seems much faster
          // sliding approach
//          var new_links = text.sli2ding(link.linkTitle.length).count(window => window == link.linkTitle)
          // regex approach
          var new_links = link.linkTitle.r.findAllMatchIn(text).length

          if (new_links > 0) {
            println(s"Page $pageTitle: Update link ${link.linkTitle} with new $new_links links")

            //TODO: Review the implementation of Link. Now is an immutable class but we might need to update it often
            //TODO: Also the performance of updating an element of a list is quite bad (consider using Vector - or mutable Link class)
            links = links.updated(i, Link(link.pageTitle, link.linkTitle, link.count + new_links))
          }
        }
        // return list of links with counts for this page
        links
      }  catch {
        case e: Exception => e.printStackTrace(); Nil
      }
    } else {
      Nil
    }
  }

  /**
    * Parses internal article links from a Page object, filtering out links that aren't to articles
   */
  def mapPagesToInternalLinksWthIndex(rdd: RDD[Page]): RDD[(String, List[Link])] = {
    rdd.map { page => (page.title, parseLinksFromPageWithCount(page.text, page.title)) }
  }

  def mapPagesToInternalLinks(rdd: RDD[Page]): RDD[Link] = {
    rdd.flatMap { page => { parseLinksFromPage(page) }}
  }
}
