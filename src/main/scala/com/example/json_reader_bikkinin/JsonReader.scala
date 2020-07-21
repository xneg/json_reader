package com.example.json_reader_bikkinin

import org.apache.spark.{SparkConf, SparkContext}
import sys.process._
import java.net.URL
import java.io.File
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.apache.spark.rdd._

object Runner {
  def run(conf: SparkConf, inputUrl: String): Unit = {
    // "https://storage.googleapis.com/otus_sample_data/winemag-data.json.tgz"
    new URL(inputUrl) #> new File("Input.tgz") !!;
    ("mkdir -p Input" #&& "tar zxvf Input.tgz -C Input") !!   

    val d = new File("Input")
    for (f <- d.listFiles().filter(f => f.getName.endsWith(".json"))) {
      val inputFile = f.toPath.toString
      printFile(conf, inputFile)
    }
  }

  def printFile(conf: SparkConf, inputFile: String): Unit = {
    val sc = new SparkContext(conf)
    val rdd = sc.textFile(inputFile)

    rdd.foreach(json => {
      implicit val formats = DefaultFormats
      println(parse(json).extract[Winemag])
    })
  }
}

case class Winemag (
  id: Option[Long] = None,
  title: Option[String] = None,
  winery: Option[String] = None,
  country: Option[String] = None,
  points: Option[Long] = None,
  variety: Option[String] = None,
  price: Option[Double] = None)