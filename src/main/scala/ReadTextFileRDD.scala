package com.scalaspark.exercises

import org.apache.spark.sql.SparkSession

object ReadTextFileRDD {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Reading Text File")
      .master("local[1]")
      .getOrCreate()
    val rdd = spark.sparkContext.textFile("data/textfile.txt") //transformation
    rdd.foreach(println) //action
    //scala.io.StdIn.readLine()

  }
}
