package com.scalaspark.exercises
import org.apache.spark.sql.SparkSession
object CreateRDD {
  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder()
      .appName("MyFirstRDD")
      .master("local[1]")
      .getOrCreate()
    val rdd =spark.sparkContext.parallelize(Seq(("karan",1),("achsaya",2),("kumar",3)))
    rdd.foreach(println)
    scala.io.StdIn.readLine()
  }
}
