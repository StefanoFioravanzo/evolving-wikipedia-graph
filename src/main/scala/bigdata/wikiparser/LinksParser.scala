package bigdata.wikiparser

import bigdata.wikiparser.PageParser.Page
import info.bliki.wiki.model.WikiModel
import org.apache.commons.lang3.StringEscapeUtils
import org.htmlcleaner.HtmlCleaner

object LinksParser {

  /**
    * Returns the links (defines by the <a/> html tag present in the page contents
    */
  def parseLinksFromPageContent(page: Page) : List[Link] = {
    if (page.text != null) {
      try {
        // convert the wikipedia text to html for easier parsing
        val html = WikiModel.toHtml(page.text)
        val cleaner = new HtmlCleaner
        val rootNode = cleaner.clean(html)
        // get all the <a> tags
        val elements = rootNode.getElementsByName("a", true)
        // parse the <a> tags and produce Link objects
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
          new Link(page.title, StringEscapeUtils.unescapeHtml4(title), 1)
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
  //TODO: In some pages there is an empty link, which then during regex matching it matches all spaces. Find out why it gets included
  /**
    * Extract all links in a page contents with recurrence counts
    * First pass: parse html and retrieve <a/> tags
    * Second pass: regex each found link to search for non-link occurrences in text
    */
  def parseLinksFromPageContentWithCount(pageText: String, pageTitle: String) : List[Link] = {
    if (pageText != null) {
      try {
        // convert the wikipedia text to html for easier parsing
        val html = WikiModel.toHtml(pageText)
        val cleaner = new HtmlCleaner
        val rootNode = cleaner.clean(html)
        // get all the <a> tags
        val elements = rootNode.getElementsByName("a", true)
        // parse the <a> tags and produce Link objects
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
          // return link title
          StringEscapeUtils.unescapeHtml4(title)
        }
        // group by link titles to get the counts
        val counts = out.groupBy(identity).mapValues(_.length)
        // create Link objects fron link title and count
        var links = (counts map {case(title:String, count:Int) => new Link(pageTitle, title, count)}).toList

        // SECOND PASS
        // Now parse the text for other occurrences of the links (not linked)
        // Exclude already parsed links pruning <a> tags
        cleaner.getProperties.setPruneTags("a")
        val rootNodeNoLinks = cleaner.clean(html)
        // article text
        val text = cleaner.getInnerHtml(rootNodeNoLinks)

        for (i <- links.indices) {
          val link = links(i)
          // search for occurrences of this link in the page text

          // sliding approach (slower than regex)
          //          var new_links = text.sli2ding(link.linkTitle.length).count(window => window == link.linkTitle)
          // regex approach
          var new_links = link.linkTitle.r.findAllMatchIn(text).length
          if (new_links > 0) {
           log.debug(s"Page $pageTitle: Update link ${link.linkTitle} with new $new_links links")
            // update Link object with new count
            // TOOD: Update directly the count wihtout creating a new object
            links = links.updated(i, new Link(link.pageTitle, link.linkTitle, link.count + new_links))
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
