package bigdata.wikiparser

import java.io.{BufferedWriter, OutputStreamWriter}

import util.control.Breaks._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.Text
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import bigdata.input.WikipediaInputFormat
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}
import org.joda.time.DateTime

// TODO: Substitute all println with LOG
object RevisionParser {

  /**
    * Represents a revision from one wiki article
    *
    * @param timestamp The time this revision was made
    * @param text      The article text at this revision
    */
  case class Revision(timestamp: DateTime, text: String)


  def readWikiRevisionsFromDump(sc: SparkContext, file: String): RDD[(String, String)] = {
    println("ReadWikiRevisions")
    val rdd = sc.newAPIHadoopFile(file, classOf[WikipediaInputFormat], classOf[Text], classOf[Text], new Configuration())
    rdd.map { case (title, revision) => (new String(title.copyBytes()), new String(revision.copyBytes())) }
  }

  /**
    * Parses the raw revision text produced by HadoopInputFormat into Revision objects
    */
  def parseRevisions(rdd: RDD[(String, String)]): RDD[(String, Revision)] = {
    rdd.mapValues { text => { parseRevisionFromRawText(text) }}
  }

  /**
    * Parses a revision from raw xml text.
    */
  def parseRevisionFromRawText(text: String): Revision = {
    // load revision into XML object
    val revision = scala.xml.XML.loadString(text)
    Revision(
      timestamp = DateTime.parse((revision \ "timestamp").text),
      text = (revision \ "text").text)
  }

  /**
    * Parses internal article links from a Revision object, filtering out links that aren't to articles
    */
  def mapPagesToInternalLinksWthIndex(rdd: RDD[(String, Revision)]): RDD[(String, (DateTime, List[Link]))] = {
    rdd.mapValues { page => (page.timestamp, LinksParser.parseLinksFromPageContentWithCount(page.text, "")) }
  }


  /**
    * Reduce all the Link lists of the same page together to procees with the final analysis
    * and disk store
    *
    * We use reduceByKey for better performance
    * See: https://databricks.gitbooks.io/databricks-spark-knowledge-base/content/best_practices/prefer_reducebykey_over_groupbykey.html
    *
    */
  def combineLinks(rdd: RDD[(String, (DateTime, List[Link]))]): RDD[(String, Iterable[(DateTime, List[Link])])] = {
    rdd.groupByKey()
  }

  /**
    * Sort all the links of an article and apply the final algo
    */
  def sortAndPrintOut(rdd: RDD[(String, Iterable[(DateTime, List[Link])])]): RDD[Int] = {
    //    rdd.mapValues { l => l.toList.sortWith(_.compareTo(_) < 0)}
    rdd.mapValues { l => l.toList.sortBy(_._1.getMillis()) }.map { case (k, v) => writeToHDFS(k, v) }
  }

  def writeToHDFS(articleTitle: String, revisionsLinks: List[(DateTime, List[Link])]): Int = {
    println("Writing to HDFS article: " + articleTitle)
    // Init hadoop file system and output stream to write to file
    // source: https://stackoverflow.com/questions/42294899/writing-to-hdfs-in-spark-scala
    val fs = FileSystem.get(SparkContext.getOrCreate().hadoopConfiguration)
    val path: Path = new Path("ewg/" + articleTitle.replaceAll("\\s", "")
    )
    if (fs.exists(path)) {
      fs.delete(path, true)
    }
    val dataOutputStream: FSDataOutputStream = fs.create(path)
    val bw: BufferedWriter = new BufferedWriter(new OutputStreamWriter(dataOutputStream, "UTF-8"))

    // ------------------------------------------------------------------------------------

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


    for ((ts, links) <- revisionsLinks) {
      breakable {

        // in case our current buffer of links is empty, we need to search
        // for the next revision with some links in it
        // This might happens in case the first revisions of the history
        // of a page do not contain any link
        if (current.isEmpty) {
          current = links
          current.foreach {
            _.from = ts
          }
          break
        }

        // In case the next revision contains exactly the same links
        // (with the same counts) - or some new that were not present before
        // as the previous revision, we just go on to the next revision
        // adding the new links
        val intersection = current.intersect(links)
        if (intersection.length == current.length) {

          // get the new links and add them to the current buffer
          links.foreach {
            _.from = ts
          }
          // links in `current` have priority over links in `links`
          current = current.union(links).distinct

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
        bw.write(linksArrayToString(ts, missing))

        val new_links = links.diff(intersection)
        new_links.foreach {
          _.from = ts
        }

        current = new_links.union(intersection).distinct
      }
    }

    bw.close()
    // return some value to make spark execute the transformation
    0
  }

  def linksArrayToString(currentTs: DateTime, links: List[Link]) : String = {
    var res = ""
    links.foreach { link =>
      res = res
        .concat("[")
        .concat(link.from.toString)
        .concat(",")
        .concat(currentTs.toString)
        .concat("] ")
        .concat(link.linkTitle)
        .concat(" ")
        .concat(link.count.toString)
        .concat("\n")
    }
    res
  }
}
