package com.learn.sparkSession

import org.apache.spark.sql.SparkSession

/**
 * Spark 2.X allows to create multiple spark sessions
 * and treat it as different job
 */
object MultipleSessionObject {
  def main(args: Array[String]): Unit = {

    val firstSparkSession = SparkSession.builder()
      .appName("Create Multiple Spark Session with Spark 2.X")
      .master("local")
      .getOrCreate()
    
    val secondSparkSession = SparkSession.builder()
      .appName("Create Multiple Spark Session with Spark 2.X")
      .master("local")
      .getOrCreate()

    val firstRDD = firstSparkSession.sparkContext.textFile("src/main/resources/csv/text02.txt")
    firstRDD.collect().foreach(println)

    val secondRDD = secondSparkSession.sparkContext.textFile("src/main/resources/csv/text04.txt")
    secondRDD.collect().foreach(println)
  }
}
