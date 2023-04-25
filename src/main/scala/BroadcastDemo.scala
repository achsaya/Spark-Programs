package com.scalaspark.exercises

import org.apache.spark.sql.SparkSession

object BroadcastDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Broadcast Demo")
      .master("local[1]")
      .getOrCreate()

    val inputRDD = spark.sparkContext.parallelize(Seq(("Emp1","1000","USA","NY"),("Emp2","2000","IND","TS"),("Emp3","2000","IND","TN"),("Emp4","3000","USA","TX"),("Emp5","4000","AUS","QUE")))
    //inputRDD.foreach(println)

    val countryData = Map(("USA","united States of America"),("IND","India"),("AUS","Australia"))
    //countryData.foreach(println)

    val statesData = Map(("NY","New York"),("TS","Telangana"),("TN","Tamil Nadu"),("TX","Texas"),("QUE","QueensLand"))
    //statesData.foreach(println)

    val broadcastStates = spark.sparkContext.broadcast(statesData)
    val broadcastCountries = spark.sparkContext.broadcast(countryData)

    val FinalRDD = inputRDD.map(f => {
      val country = f._3
      val state = f._4
      val fullCountry = broadcastCountries.value.get(country).get
      val fullState = broadcastStates.value.get(state).get
      (f._1, f._2, fullCountry, fullState)
    })

    println(FinalRDD.collect().mkString("\n"))
    scala.io.StdIn.readLine()

  }

}
