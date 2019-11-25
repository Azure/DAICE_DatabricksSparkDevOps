package com.microsoft.spark.example

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD

import org.apache.spark.sql._

object SparkSimpleApp {

  def rdd_csv_col_count(inputRDD: RDD[String]): RDD[Integer] = {
    return inputRDD.map(s => s.split(",").size)
  }

  def main (arg: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("SparkSimpleApp").getOrCreate()
    import spark.implicits._
    val dataInput = arg(0)
    val dataOutput = arg(1)

    val rdd = spark.sparkContext.textFile(dataInput)

    //find the rows which have only one digit in the 7th column in the CSV
    val rdd1 = rdd_csv_col_count(rdd)

    rdd1.toDF().write.mode(SaveMode.Overwrite).csv(dataOutput)
  }
}