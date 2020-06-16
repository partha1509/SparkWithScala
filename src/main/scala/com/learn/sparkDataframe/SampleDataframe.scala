package com.learn.sparkDataframe

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{DateType, IntegerType, StringType, StructField, StructType}

/**
 * DataFrame can be created from RDD, File, Database
 */
object SampleDataframe extends App {
  val spark = SparkSession.builder()
    .appName("Create DataFrame")
    .master("local")
    .getOrCreate()
  val dataFrameFromCSV = spark.read
    .option("header", "true")
    .csv("src/main/resources/zipcodes.csv")
  dataFrameFromCSV.collectAsList().forEach(println)

  val sampleRDD = spark.sparkContext.textFile("src/main/resources/stream.csv")
  val schema = StructType(
    StructField("TotalCost", IntegerType, true) ::
      StructField("BirthDate", StringType, true) ::
      StructField("Gender", StringType, true) ::
      StructField("TotalChildren", IntegerType, true) ::
      StructField("ProductCategoryName", StringType, true) :: Nil
  )
  val dataFrameRDD = sampleRDD.map(line => Row(line))
  val dataFrameFromRDD = spark.createDataFrame(dataFrameRDD, schema)
  dataFrameFromRDD.collectAsList().forEach(println)

}
