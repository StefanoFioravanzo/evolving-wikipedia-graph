package bigdata.wikiparser

import java.io.{BufferedWriter, OutputStreamWriter}

import util.control.Breaks._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.Text
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import bigdata.input.WikipediaInputFormat
import bigdata.wikiparser.LinksParser.{Link, Linkclass}
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
    rdd.mapValues { text => {
      parseRevisionFromRawText(text)
    }
    }
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
  def sortAndPrintOut(sc: SparkContext, rdd: RDD[(String, Iterable[(DateTime, List[Linkclass])])]): Unit = {
    //    rdd.mapValues { l => l.toList.sortWith(_.compareTo(_) < 0)}
    rdd.mapValues { l => l.toList.sortBy(_._1.getMillis()) }.map { case (k, v) => writeToHDFS(sc, k, v) }
  }

  def writeToHDFS(sc: SparkContext, articleTitle: String, revisionsLinks: List[(DateTime, List[Linkclass])]): Unit = {
    // Init hadoop file system and output stream to write to file
    // source: https://stackoverflow.com/questions/42294899/writing-to-hdfs-in-spark-scala
    val fs = FileSystem.get(sc.hadoopConfiguration)
    val path: Path = new Path("/ewg/" + articleTitle.replaceAll("\\s", "")
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

      missing = current - intersection // get all those links that either are not anymore in links or that have a different count (implement equals in Linkclass)
      save missing to file (or in data structure) with timestamp range [missing.from, ts]
      new = links - intersection   // all links that are being added or with a new count
      for n in new:
        n.from <- ts
      make sure the intersection elements are the ones from current (from field needs to be assigned)
      current = new + intersection
     */

    breakable {
      for ((ts, links) <- revisionsLinks) {
        val intersection = current.intersect(links)
        if (intersection.length == current.length) {
          break
        }

        val missing = current.diff(intersection)
        bw.write(linksArrayToString(ts, missing))

        val new_links = links.diff(intersection)
        new_links.foreach {
          _.from = ts
        }

        current = new_links.union(intersection)
      }
    }

    //    val fs = FileSystem.get(sc.hadoopConfiguration)
    //    val file = fs.globStatus(new Path("path/file.csv/part*"))(0).getPath.getName
    //
    //    fs.rename(new Path("csvDirectory/" + file), new Path("mydata.csv"))
    //    fs.delete(new Path("mydata.csv-temp"), true)

    bw.close()
  }

  def linksArrayToString(currentTs: DateTime, links: List[Linkclass]) : String = {
    val res = ""
    links.foreach { link =>
      res
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
