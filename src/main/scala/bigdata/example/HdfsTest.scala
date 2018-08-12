package bigdata.example

import org.apache.spark.sql.SparkSession

object HdfsTest {

  def main(args: Array[String]) {

    val spark = SparkSession
      .builder
//      .master("spark://localhost:7077")
      .appName("HdfsExample")
      .getOrCreate()

    spark.read.option("header", "true")
      .csv("hdfs://hadoop-namenode/tmp/persons.csv")
//      .csv("hdfs://172.19.0.2/tmp/persons.csv")
      .show()

    spark.stop()
  }
}
