package com.example.json_reader_bikkinin

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql._

object BostonCrimeRunner {
  def run(conf: SparkConf, pathToCrime: String, pathToCodes: String, outputFolder: String): Unit = {
    val spark = SparkSession.builder.config(conf).getOrCreate()    
    import spark.implicits._ 

    val crimes = spark.read.format("csv")
      .option("header", "true")
      .load(pathToCrime)

    val crimeCodes = spark.read.format("csv")
      .option("header", "true")
      .load(pathToCodes)

    val windowSpec  = Window.partitionBy("DISTRICT").orderBy(desc("count"))

    val frequent_crime_types = crimes
        .join(crimeCodes, ($"OFFENSE_CODE" cast "Int") === ($"CODE" cast "Int"))
        .groupBy("DISTRICT", "OFFENSE_CODE", "NAME")
        .count()
        .withColumn("row_number", row_number() over windowSpec)
        .withColumn("offense", split($"NAME", " ")(0))
        .filter($"row_number" <= 3)
        .groupBy("DISTRICT")
        .agg(concat_ws(", ", collect_list($"offense")) as "frequent_crime_types")

    val crimes_total = crimes
        .groupBy("DISTRICT")
        .agg(count("*") as "crimes_total", avg("lat") as "lat", avg("long") as "lng")

    val crimes_monthly = crimes
        .select($"DISTRICT", concat($"YEAR", $"MONTH") as "month")
        .groupBy("DISTRICT", "month")
        .count()
        .groupBy("DISTRICT")
        .agg(callUDF("percentile_approx", col("count"), lit(0.5)) as "crimes_monthly")
//     .agg(percentile_approx("count") as "crimes_monthly")
//     .show()
//     .agg(count("*") as "crimes_total", avg("lat"), avg("long") as "lng")
//     .stat.approxQuantile("count", Array(0.5), 0.25)
//     .agg(percentile_approx("*") as "crimes_monthly")

    val joined_crimes = crimes_total
        .join(crimes_monthly, "DISTRICT")
        .join(frequent_crime_types, "DISTRICT")

    joined_crimes.write.parquet(outputFolder)
  }
}