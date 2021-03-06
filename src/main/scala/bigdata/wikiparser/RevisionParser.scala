package bigdata.wikiparser

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.Text
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}
import org.apache.logging.log4j.{Level, LogManager, Logger}
import org.apache.logging.log4j.core.config.Configurator
import org.joda.time.DateTime
import bigdata.input.WikipediaInputFormat

object RevisionParser {

//  @transient lazy val log = org.apache.log4j.LogManager.getLogger("RevisionParser")
  val log: Logger = LogManager.getLogger("MyLogger")
  Configurator.setLevel(log.getName, Level.INFO)
  // Load custom configuration from src/main/resources/application.conf
  val myConf: Config = ConfigFactory.load()
  val env: String = myConf.getString("ewg.env")

  /**
    * Represents a revision from one wiki article
    *
    * @param timestamp The time this revision was made
    * @param text      The article text at this revision
    */
  case class Revision(timestamp: DateTime, text: String)


  /**
    * Init the WikipediaInputFormat Hadoop distributed reader to stream in the wikipedia input file.
    * Each input (a single <articleTitle, revision> couple) is returned as an element of an RDD
    *
    * @param sc SparkContext
    * @return file Wikipedia input file path
    */
  def readWikiRevisionsFromDump(sc: SparkContext, file: String): RDD[(String, (DateTime, List[Link]))] = {
    log.info("ReadWikiRevisions")
    val rdd = sc.newAPIHadoopFile(file, classOf[WikipediaInputFormat], classOf[Text], classOf[Text], new Configuration())
    rdd.map { case (title, revision) => parseLinks(title, revision) }
  }

  /**
    * Get one revision and parse its contents to produce a list of links
    */
  def parseLinks(articleTitle: Text, revisionXml: Text): (String, (DateTime, List[Link])) = {
    val title = new String(articleTitle.copyBytes())
    val xml = new String(revisionXml.copyBytes())
    val tmp = scala.xml.XML.loadString(xml)
    // create Revision object from raw data
    val revision = Revision(
      timestamp = DateTime.parse((tmp \ "timestamp").text),
      text = (tmp \ "text").text)

    // parse revision text and produce list of links
    (title, (revision.timestamp, LinksParser.parseLinksFromPageContentWithCount(revision.text)))
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
    * Sort all the links of an article and apply the final edge producing algorithm
    */
  def createFinalOutput(rdd: RDD[(String, Iterable[(DateTime, List[Link])])]): RDD[Int] = {
    //    rdd.mapValues { l => l.toList.sortWith(_.compareTo(_) < 0)}
    // sort all the links of an sticle by the timestamp (using getMillis()) and map each sorted
    // list to writeToHDFS method.
    rdd.map { case (k, v) => {
      val sorted = v.toList.sortBy(_._1.getMillis())
      writeToHDFS(k, sorted)
    }}
  }

  /**
    * Write to a single article file (named after the articleTitle) the list of edges
    * of the temporal graph.
    * Each edge comes in the form:
    *   [start_date, end_data] linkPageTitle count
    *
    * Each link count can change through time, so we keep track of these changes as 
    * we scan the list of links and write to file a new edge each time a link disappears or
    * its count changes.
    */
  def writeToHDFS(articleTitle: String, revisionsLinks: List[(DateTime, List[Link])]): Int = {
    log.info("Writing to HDFS article: " + articleTitle)

    // Init hadoop file system and output stream to write to file
    val path = myConf.getString(s"ewg.$env.output-path")
    val fileName = articleTitle.replaceAll("\\s", "")
    // open an output stream to either HDFS or local file system based on configuration
    val outputStream = openHDFSFile(path, fileName)
    val writeToFile = outputStream.writeBytes _

    LinksParser.createEWGEdges(writeToFile, revisionsLinks)

    outputStream.close()
    // return some value to make spark execute the transformation
    0
  }

  /**
  Open an output stream to a new article file
    Based on application.conf environment configuration, the function
    will open a stream to a local file or an file in HDFS.
    */
  def openHDFSFile(filePath:String, fileName: String): FSDataOutputStream = {
    // ====== Init HDFS File System Object

    var fs: FileSystem = null
    //Get the filesystem - HDFS
    if (env == "local") {
      fs = FileSystem.get(SparkContext.getOrCreate().hadoopConfiguration)
    } else if (env == "dist") {
      var conf = new Configuration()
      // Set FileSystem URI
      conf.set("fs.defaultFS", "hdfs://hadoop-namenode")
      // Set HADOOP user
      System.setProperty("HADOOP_USER_NAME", "hdfs")
      System.setProperty("hadoop.home.dir", "/")
      fs = FileSystem.get(URI.create("hdfs://hadoop-namenode"), conf)
    } else {
      throw new Exception("Wrong env conf")
    }

    //==== Create folder if not exists

    val workingDir = fs.getWorkingDirectory
    val newFolderPath = new Path(filePath)
    if (!fs.exists(newFolderPath)) {
      // Create new Directory
      fs.mkdirs(newFolderPath)
    }

    //==== Write file

    // Create a path
    val hdfswritepath = new Path(newFolderPath + "/" + fileName)
    // Init and return output stream
    fs.create(hdfswritepath)
  }


  /**
    * Transform Link list to printable edges string
    */
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
