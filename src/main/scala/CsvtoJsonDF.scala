package com.scalaspark.exercises

import org.apache.spark.sql.{SaveMode, SparkSession}

object CsvtoJsonDF {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Second DataFrame")
      .master("local[1]")
      .getOrCreate()

    import spark.implicits._
    val df = spark.read.option("header", true)
      .csv("data/emp.csv")
    df.show()
    //df.toJSON.show()
    df.write.json("data/empnew1")

  }

}
