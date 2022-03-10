package org.prueba

import org.apache.spark
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.{col, countDistinct}
import org.apache.spark.sql.types.{BooleanType, FloatType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{functions => F}

object Prueba {
  def main(args: Array[String]) {


    val spark = SparkSession
      .builder.master("local[1]")
      .appName("MnMCount")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val csvFile="C:\\Users\\lucia.riera\\IdeaProjects\\Prueba\\data\\departuredelays.csv"

    val schema = "date STRING, delay INT, distance INT, origin STRING, destination STRING"

    val df = spark.read.schema(schema)
      .option("header", "true")
      .csv(csvFile)
    // Create a temporary view
    df.createOrReplaceTempView("us_delay_flights_tbl")

    val formato = df.withColumn("Date", to_timestamp(col("date"), "MMddhhmm"))

    spark.sql("""SELECT date, delay, origin, destination
                FROM us_delay_flights_tbl
                WHERE ORIGIN = 'ABE' AND DESTINATION = 'ATL'""").show(10)

    formato.show()





  }
}
