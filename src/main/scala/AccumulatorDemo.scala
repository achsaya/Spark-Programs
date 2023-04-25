package com.scalaspark.exercises

import org.apache.spark.sql.SparkSession

object AccumulatorDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Accumulator Demo")
      .master("local[1]")
      .getOrCreate()

    val LongAcc = spark.sparkContext.longAccumulator("Sumaccumulator")
    val DoubleAcc = spark.sparkContext.doubleAccumulator("Sumdoubleaccumulator")
    val rdd = spark.sparkContext.parallelize(Array(1,2,3))
    val rdd1 = spark.sparkContext.parallelize(Array(1.23,2.3435,3.53345))
    rdd1.foreach(y => DoubleAcc.add(y))
    rdd.foreach(x => LongAcc.add(x))
    println(LongAcc.value)
    println(DoubleAcc.value)
    //scala.io.StdIn.readLine()
  }

}
