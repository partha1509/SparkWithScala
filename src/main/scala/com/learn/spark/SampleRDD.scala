package com.learn.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object SampleRDD {
  def main(arg: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[1]")
      .appName("SparkWithScala")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    println("##Reading single file into RDD")
    val singleFileRDD = spark.sparkContext.textFile("src/main/resources/csv/text01.txt")
    singleFileRDD.collect().foreach(d => println(d))

    println("##Reading multiple files into RDD")
    val multipleFileRDD = spark.sparkContext.textFile("src/main/resources/csv/text*.txt")
    println(multipleFileRDD.getClass)
    multipleFileRDD.collect().foreach(d => println(d))

    println("##Reading two files into RDD")
    val twoFileRDD = spark.sparkContext.textFile("src/main/resources/csv/text01.txt," + "src/main/resources/csv/text02.txt")
    println(twoFileRDD.getClass)
    twoFileRDD.collect().foreach(d => println(d))

    println("## Reading whole text file")
    val fullFileRDD: RDD[(String, String)] = spark.sparkContext.wholeTextFiles("src/main/resources/csv/text01.txt")
    println(fullFileRDD.getClass)
    fullFileRDD.foreach(d => {
      println(d._1 + "=>" + d._2)
    })
  }
}
