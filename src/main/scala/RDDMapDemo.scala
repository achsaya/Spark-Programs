package com.scalaspark.exercises

import org.apache.spark.sql.SparkSession

object RDDMapDemo {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("DemoMap Method")
      .master("local[1]")
      .getOrCreate()

    val data = Seq("Project Gutemburg's",
      "Alice in wonderland",
      "Adventure in wonderland")

    import spark.sqlContext.implicits._
    val df = data.toDF("data")
    df.show()

    val mapDF = df.map(fun => {
      fun.getString(0).split(" ")
    })
    mapDF.show()

  }
}
