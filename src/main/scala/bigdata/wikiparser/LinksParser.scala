package bigdata.wikiparser

import java.lang

import bigdata.wikiparser.PageParser.Page
import info.bliki.wiki.model.WikiModel
import org.apache.commons.lang3.StringEscapeUtils
import org.htmlcleaner.HtmlCleaner
import org.joda.time.DateTime

object LinksParser {

  /**
    * Represent a link from one wiki article to another
    *
    * @param pageTitle Title of the current article
    * @param linkTitle Title of the linked article
    * @param count The count of links with this title in the page
    */
  case class Link(pageTitle: String, linkTitle: String, count: Int)

  class Linkclass(
                   var pageTitle: String,
                   var linkTitle: String,
                   var count: Int = 0,
                   var from: DateTime = null
                 ) {

    //NOTE: from field is not included in equal comparison!

    def canEqual(other: Any): Boolean = other.isInstanceOf[Linkclass]

    override def equals(other: Any): Boolean = other match {
      case that: Linkclass =>
        (that canEqual this) &&
          pageTitle == that.pageTitle &&
          linkTitle == that.linkTitle &&
          count == that.count
      case _ => false
    }

    override def hashCode(): Int = {
      val state = Seq(pageTitle, linkTitle, count)
      state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
    }

  }

  /**
    * Simple version: returns the links (defines by the <a/> html tag
    * present in the page contents
    */
  def parseLinksFromPageContent(page: Page) : List[Link] = {
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
    * Extract all links in a page contents with recurrence counts
    * First pass: parse html and retrieve <a/> tags
    * Second pass: regex each found link to search for non-link occurrences in text
    */
  def parseLinksFromPageContentWithCount(pageText: String, pageTitle: String) : List[Link] = {
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
//            println(s"Page $pageTitle: Update link ${link.linkTitle} with new $new_links links")

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
}
