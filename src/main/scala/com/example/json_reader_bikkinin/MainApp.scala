package com.example.json_reader_bikkinin

import org.apache.spark.{SparkConf, SparkContext}

object JsonReaderLocal extends App{
  val inputUrl = args(0)
  
  val conf = new SparkConf()
    .setMaster("local")
    .setAppName("my awesome app")

  Runner.run(conf, inputUrl)
}

/**
  * Use this when submitting the app to a cluster with spark-submit
  * */
object JsonReader extends App{
  val inputUrl = args(0)

  // spark-submit command should supply all necessary config elements
  Runner.run(new SparkConf(), inputUrl)
}