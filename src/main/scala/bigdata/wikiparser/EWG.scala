package bigdata.wikiparser

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object EWG {
  // TODO: Substitute all println with LOG
  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("EvolvingWikipediaGraph")
      .setMaster("local")
    val sc = new SparkContext(conf)

    parseRevisions(sc)
  }

  def parseRevisions(sc: SparkContext): Unit = {
    val revisionsFile = "data/pages_history_sample.xml"

    // returns an RDD[(title: String, revision: String)]
    val links = Parser.readWikiRevisionsFromDump(sc, revisionsFile)
    val collected = links.collect()
    println("Done.")
  }

  def parsePagesLinks(sc: SparkContext): Unit = {
    // Should be some file on your system
    val wikiFile = "data/enwiki-latest-pages-articles1.xml-p10p30302.2megs"
    //    val wikiFile = "data/enwiki-latest-pages-articles1.xml-p10p30302.bz2.2megs"
    //    val wikiFile = "hdfs://hadoop-namenode/bigdata/enwiki-latest-pages-articles1.xml-p10p30302.2megs"

    val raw_pages_text = Parser.readWikiPagesFromDump(sc, wikiFile)
    val pages = Parser.parsePagesWithIndex(raw_pages_text)

    // pages is now a RDD[(long, page)], we need a RDD[page]
    val new_pages: RDD[Parser.Page] = pages.map{case (_, page) => page}
    val links = Parser.mapPagesToInternalLinksWthIndex(new_pages)

    val collected_pages = pages.collect()
    val collected_new_pages = new_pages.collect()
    val collected_links = links.collect()

    //    links.coalesce(1).saveAsTextFile("hdfs://hadoop-namenode/bigdata/output")

    println("Done.")
  }
}