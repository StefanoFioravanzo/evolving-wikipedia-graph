package bigdata.wikiparser

import org.apache.logging.log4j.{Level, LogManager, Logger}
import org.apache.logging.log4j.core.config.Configurator
import org.apache.logging.log4j.core.config.DefaultConfiguration
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import com.typesafe.config.{Config, ConfigFactory}
import org.joda.time.DateTime

/*
Notes on improvements:

In case of memory problems, we could use an integer as ID instead of the article title
and use another map from id to title to get back the title in the end.
 */

/**
  * Main application entrypoint
  */
object EWG {

  val log: Logger = LogManager.getLogger("MyLogger")
  // Load custom configuration from src/main/resources/application.conf
  val myConf: Config = ConfigFactory.load()
  val env: String = myConf.getString("ewg.env")

  def main(args: Array[String]) {
    // Init Log4j
    Configurator.initialize(new DefaultConfiguration)
    Configurator.setLevel(log.getName, Level.INFO)
    log.info("Start parsing process")
    val conf = new SparkConf().setAppName("EvolvingWikipediaGraph")
    if (env == "local") {
      conf.setMaster("local[1]")
//      conf.set("spark.cores.max", "1")
    }

    val sequential = myConf.getBoolean(s"ewg.$env.sequential")
    if (sequential) {
      sequentialParseRevisions()
    } else {
      val sc = new SparkContext(conf)
      parseRevisions(sc)
    }
  }

  def sequentialParseRevisions(): Unit = {
    val revisionsFile = myConf.getString(s"ewg.$env.sequential-revisions-file")
    RevisionParserSequential.openLocalFiles()
  }

  /**
    * Start revisioin parsing job. This function will start a series of 
    * Spark jobs th at stream in wikipedia data, parse all the article revisions 
    * and produce a series of files describing the evolving graph
    */
  def parseRevisions(sc: SparkContext): Unit = {
    val revisionsFile = myConf.getString(s"ewg.$env.revisions-file")

    // returns an RDD[(title: String, revision: String)]
    // stream in all the revisions from the input file and parse the revisions text to produce a list of links for each revision
    val revisionsLinks: RDD[(String, (DateTime, List[Link]))] = RevisionParser.readWikiRevisionsFromDump(sc, revisionsFile)
    // Group links to "merge" together all the revisions of the same article
    val links_comb = RevisionParser.combineLinks(revisionsLinks)
    // Sort all the links of all the revisions of an article
    // Parse the sorted list and write to HDFS an edge file for that article
    RevisionParser.createFinalOutput(links_comb).collect()

    log.info("Done.")
  }

  /**
    * Parse a simple article wikipedia file (no revision history)
    * to retrieve the article pages
    */
  def parsePagesLinks(sc: SparkContext): Unit = {
    // Should be some file on your system
    val wikiFile = myConf.getString(s"ewg.$env.wiki-pages-file")

    // Stream in all the articles from the input file
    val raw_pages_text = PageParser.readWikiPagesFromDump(sc, wikiFile)
    // Convert raw page text to a Page object
    val pages = PageParser.parsePagesWithIndex(raw_pages_text)

    // pages is now a RDD[(long, page)], we need a RDD[page]
    val new_pages: RDD[PageParser.Page] = pages.map{case (_, page) => page}
    // Retrieve all the links from the page
    val links = PageParser.mapPagesToInternalLinksWthIndex(new_pages)

    // collect resuts for debugging
    val collected_pages = pages.collect()
    val collected_new_pages = new_pages.collect()
    val collected_links = links.collect()

    //    links.coalesce(1).saveAsTextFile("hdfs://hadoop-namenode/bigdata/output")
    log.info("Done.")
  }
}