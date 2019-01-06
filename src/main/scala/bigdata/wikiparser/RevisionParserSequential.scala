package bigdata.wikiparser

import java.io.{File, PrintWriter}
import java.text.SimpleDateFormat

import bigdata.wikiparser.RevisionParser.{Revision, env, log, myConf}
import org.joda.time.DateTime

import scala.io.Source
import reflect.io.{Path => LocalPath}
import scala.xml.{Elem, MetaData, Node, Text}
import scala.xml.pull.{EvElemEnd, EvElemStart, EvText, XMLEventReader}

/**
  * Wikipedia Revisions Parses with sequential (no spark involved) approach
  */
object RevisionParserSequential {
  implicit class Regex(sc: StringContext) {
    def r = new util.matching.Regex(sc.parts.mkString, sc.parts.tail.map(_ => "x"): _*)
  }

  /**
    * Open all files based on path wildcard
    */
  def openLocalFiles(): Unit = {
    val path = myConf.getString(s"ewg.$env.sequential-revisions-path")
    val files = (LocalPath(path).toDirectory.files map (_.name) flatMap { x =>  x match  { case r"history_revisions_\d+\.xml" => Some(x) case _ => None}}).toList
    files.foreach(
      x => {
        log.info(s"processing file $x")
        sequentialProcessing(s"$path$x")
      }
    )
  }

  /**
    * Sequential version of revisions parsing.
    * This method streams in the XML file content, applying the same methods
    * used by Spark to parse the links from revisions.
    * In the end the results are printed to file.
    * All is done without any parallelization.
    */
  def sequentialProcessing(filePath: String): Unit = {
    // open file with XML streamer
    val xml = new XMLEventReader(Source.fromFile(filePath))

    var insidePage = false
    var currentPageTitle = ""
    var articleRevisions : List[(DateTime, List[Link])] = List[(DateTime, List[Link])]()
    var pw : PrintWriter = null

    // define timestamp condition to accept revision
    val date_pattern: String = "dd/MM/yyyy-hh:mm:ss"
    val strdate: String = "01/01/1960-00:00:00"
    val df = new SimpleDateFormat(date_pattern)
    val date = df.parse(strdate)
    val dt = new DateTime(date.getTime)
    // to output the date: df.format(today)

    while (xml.hasNext) {
      // get next node in xml
      val elem = xml.next
      elem match {
        // if element is a starting tag
        case EvElemStart(_, label, attrs, _) =>
          if (label == "page") {
            insidePage = true
          }
          if (label == "title" && insidePage) {
            // get the title of the article
            xml.next match {
              case EvText(text) =>
                currentPageTitle = text
                log.info(s"Parsing page $currentPageTitle")
                // open a new file for this article
                val f = new File(myConf.getString(s"ewg.$env.output-path") + "/" + currentPageTitle.replaceAll("\\s", "") + ".txt")
                f.mkdirs()
                f.delete()
                f.createNewFile()
                pw = new PrintWriter(f)
                // delete previous content
                articleRevisions = List[(DateTime, List[Link])]()
            }
          }
          if (label == "redirect") {
            // this article is just a redirect, skip to next article
            insidePage = false
            log.info(s"Page is a redirect. Skipping.")
          }
          if (label == "revision" && insidePage) {
            // get all the revision tags contents
            val rev = subTree(xml, label, attrs)
            // parse revision into Revision object
            val rev_timestamp = DateTime.parse((rev \ "timestamp").text)
            // if rev_timestamp > dt
            if (rev_timestamp.compareTo(dt) > 0) {
//              log.info(s"revision accepted")
              val revision = Revision(
                timestamp = rev_timestamp,
                text = (rev \ "text").text)
              // extract links from revision text and add them to article list of revisions
              articleRevisions =  (revision.timestamp, LinksParser.parseLinksFromPageContentWithCount(revision.text)) :: articleRevisions
            }
          }
        case EvElemEnd(_, label) =>
          if (label == "page" && insidePage) {
            // article revisions end.
            // first sort links, then parse and print out
            articleRevisions = articleRevisions.sortBy(_._1.getMillis())
            LinksParser.createEWGEdges(pw.write, articleRevisions)
            // close article file
            pw.close()
            insidePage = false
          }
        case EvText(text) =>
        case _ => null
      }
    }
  }

  /**
    * Extract all the child XML elements from the specified parent tag node
    * @param it XMLEventReader of the XML file
    * @param tag parent tag
    * @param attrs parent attributes
    * @return Node XML tree with tag node as parent
    */
  def subTree(it: XMLEventReader, tag: String, attrs: MetaData): Node = {
    var children = List[Node]()

    while (it.hasNext) {
      it.next match {
        case EvElemStart(_, t, a, _) => {
          children = children :+ subTree(it, t, a)
        }
        case EvText(t) => {
          children = children :+ Text(t)
        }
        case EvElemEnd(_, t) => {
          return new Elem(null, tag, attrs, xml.TopScope, children: _*)
        }
        case _ =>
      }
    }
    null // this shouldn't happen with good XML
  }
}
