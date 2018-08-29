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

object EWG {

  val log: Logger = LogManager.getLogger("MyLogger")
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

  def parseRevisions(sc: SparkContext): Unit = {
    val revisionsFile = myConf.getString(s"ewg.$env.revisions-file")

    // returns an RDD[(title: String, revision: String)]
    val rawRevisions = RevisionParser.readWikiRevisionsFromDump(sc, revisionsFile)
    val revisions = RevisionParser.parseRevisions(rawRevisions)

    val links = RevisionParser.mapPagesToInternalLinksWthIndex(revisions)
    val links_comb = RevisionParser.combineLinks(links)
    RevisionParser.sortAndPrintOut(links_comb).collect()

    val rawRevisionsCol = rawRevisions.collect()
    val revisionsCol = revisions.collect()
    log.info("Done.")
  }

  def parsePagesLinks(sc: SparkContext): Unit = {
    // Should be some file on your system
    val wikiFile = myConf.getString(s"ewg.$env.wiki-pages-file")

    val raw_pages_text = PageParser.readWikiPagesFromDump(sc, wikiFile)
    val pages = PageParser.parsePagesWithIndex(raw_pages_text)

    // pages is now a RDD[(long, page)], we need a RDD[page]
    val new_pages: RDD[PageParser.Page] = pages.map{case (_, page) => page}
    val links = PageParser.mapPagesToInternalLinksWthIndex(new_pages)

    val collected_pages = pages.collect()
    val collected_new_pages = new_pages.collect()
    val collected_links = links.collect()

    //    links.coalesce(1).saveAsTextFile("hdfs://hadoop-namenode/bigdata/output")

    log.info("Done.")
  }
}