package com.scalaspark.exercises

import org.apache.spark.sql.SparkSession

object FirstDataFrammeDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("First DataFrame")
      .master("local[1]")
      .getOrCreate()

    val data = Seq(("Anil","1000",29,"Hyderabad"),
      ("Kavin","100000",22,"Chennai"),
      ("Raj","10000",20,"Madurai"),
      ("Yaswanth","20000",21,"Trichy"),
      ("Abi","20000",27,"Anal"))

    val columns = Seq("Name","Salary","Age","Place")

    val rdd = spark.sparkContext.parallelize(data)

    import spark.implicits._
    val df =rdd.toDF(columns:_*)
    df.show()
  }

}
