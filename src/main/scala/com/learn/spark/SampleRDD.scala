package com.learn.spark
import org.apache.spark.sql.SparkSession

object SampleRDD  {
  def main(arg: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[1]")
      .appName("SparkWithScala")
      .getOrCreate()

  }
}
