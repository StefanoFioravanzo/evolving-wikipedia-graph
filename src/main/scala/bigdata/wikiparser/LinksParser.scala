package bigdata.wikiparser

import bigdata.wikiparser.PageParser.Page
import bigdata.wikiparser.RevisionParser.linksArrayToString
import info.bliki.wiki.model.WikiModel
import org.apache.commons.lang3.StringEscapeUtils
import org.htmlcleaner.HtmlCleaner
import org.joda.time.DateTime

import scala.util.control.Breaks.{break, breakable}

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
           // log.debug(s"Page $pageTitle: Update link ${link.linkTitle} with new $new_links links")
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

  def createEWGEdges(writeToFile: String => Unit, revisionsLinks: List[(DateTime, List[Link])]): Unit = {
    // ------------------------------------------------------------------------------------
    // get first link in the list
    var current = revisionsLinks.head._2
    // assign first revision date to each link (starting datetime)
    current.foreach {
      _.from = revisionsLinks.head._1
    }

    // PSEUDO-CODE
    /*
    for ts, links in revisionLinks:
      intersection = intersect(links, current)
      if (intersection == current)  // new links don't change
        continue to next revision

      missing = current - intersection // get all those links that either are not anymore in links or that have a different count (implement equals in Link)
      save missing to file (or in data structure) with timestamp range [missing.from, ts]
      new = links - intersection   // all links that are being added or with a new count
      for n in new:
        n.from <- ts
      make sure the intersection elements are the ones from current (from field needs to be assigned)
      current = new + intersection
     */


    // iterate over the link
    for ((ts, links) <- revisionsLinks) {
      // this `breakable` construct is the scala way of providing the `continue`
      // keyword inside a for loop
      breakable {

        // in case our current buffer of links is empty, we need to search
        // for the next revision with some links in it
        // This might happen in case the first revisions of the history
        // of a page do not contain any link
        if (current.isEmpty) {
          current = links
          // assign current timestamp
          current.foreach {
            _.from = ts
          }
          // (continue to next iteration)
          break
        }

        // In case the next revision contains exactly the same links
        // (with the same counts) - or some new that were not present before -
        // as the previous revision, we just go on to the next revision
        // adding the new links
        val intersection = current.intersect(links)
        if (intersection.length == current.length) {
          // get the new links and add them to the current buffer
          links.foreach {
            _.from = ts
          }
          // links in `current` have priority over links in `links`
          // need to use distict because `union()` does not automatically
          // discard equal objects
          current = current.union(links).distinct
          // (continue to next iteration)
          break
        }

        // TODO: The current algorithm will print out links with the same counts
        // multiple times in case the link increases its count and then in the future
        // decreases it again. Need to evaluate how to store this info. Whether as two different
        // edges or with an edge w/ multiple date ranges.

        // Write to HDFS all the links that are missing from the new revision
        // This includes both links that just changed the count and links that might
        // not be present anymore
        val missing = current.diff(intersection)
        // outputStream write function
        writeToFile(linksArrayToString(ts, missing))

        val new_links = links.diff(intersection)
        new_links.foreach {
          _.from = ts
        }

        // update current link buffer with the new_links
        current = new_links.union(intersection).distinct
      }
    }
  }
}
