package bigdata.example

import org.apache.spark.{SparkConf, SparkContext}

object SimpleApp {
  def main(args: Array[String]) {
    // Should be some file on your system
    val logFile = "data/text.md"
    val conf = new SparkConf()
      .setAppName("Simple Application")
      .setMaster("local")
//      .setMaster("spark://localhost:7077")
//      .set("spark.local.dir", "/spark-tmp")
//      .set("spark.driver.memory", "1g")
//      .set("spark.executor.memory", "1g")
//      .set("spark.executor.cores", "2")
    val sc = new SparkContext(conf)
    val logData = sc.textFile(logFile, 2).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println("Lines with a: %s, Lines with b: %s".format(numAs, numBs))
  }
}
