package bigdata.wikiparser

import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import com.typesafe.config.{ConfigFactory, Config}

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
  // Load custon configuration from src/main/resources/application.conf
  val myConf: Config = ConfigFactory.load()
  val env: String = myConf.getString("ewg.env")

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("EvolvingWikipediaGraph")
    if (env == "local") {
      conf.setMaster("local")
    }
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    parseRevisions(sc)
  }

  /**
    * Start revisioin parsing job. This function will start a series of 
    * Spark jobs th at stream in wikipedia data, parse all the article revisions 
    * and produce a series of files describing the evolving graph
    */
  def parseRevisions(sc: SparkContext): Unit = {
    val revisionsFile = myConf.getString(s"ewg.$env.revisions-file")

    // returns an RDD[(title: String, revision: String)]
    // stream in all the revisions fron the input file
    val rawRevisions = RevisionParser.readWikiRevisionsFromDump(sc, revisionsFile)
    // Convert raw revision text to Revision object, getting text value and timestamp
    val revisions = RevisionParser.parseRevisions(rawRevisions)
    // Parse revision text to get a list of links with their appearance count
    val links = RevisionParser.mapPagesToInternalLinksWthIndex(revisions)
    // Group links to "merge" together all the revisions of the same article
    val links_comb = RevisionParser.combineLinks(links)
    // Sort all the links of all the revisions of an article
    // Parse the sorted list and write to HDFS an edge file for that article
    RevisionParser.sortAndPrintOut(links_comb).collect()

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