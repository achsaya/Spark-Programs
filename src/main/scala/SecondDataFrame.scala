package com.scalaspark.exercises

import org.apache.spark.sql.SparkSession

object SecondDataFrame {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Second DataFrame")
      .master("local[1]")
      .getOrCreate()

    import spark.implicits._
    val df = spark.read.option("header",true)
      .csv("data/emp.csv")
    df.show()

    df.select("Name","Age").show()

    df.select("Name", "Salary", "Age", "Place").where("age>21").show()

  }

}
