package org.prueba
import org.apache.spark.sql.{functions => F}
import org.apache.spark
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.Decimal.MAX_INT_DIGITS
import org.apache.spark.sql.types.{BooleanType, FloatType, IntegerType, StringType, StructField, StructType}
object Vuelos {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder.master("local[1]")
      .appName("Vuelos")
      .getOrCreate()
    // Path to data set




    val csvFile="C:\\Users\\lucia.riera\\IdeaProjects\\Prueba\\data\\departuredelays.csv"

    val schema = "date STRING, delay INT, distance INT, origin STRING, destination STRING"

    val df = spark.read.schema(schema)
      .option("header", "true")
      .csv(csvFile)
    // Create a temporary view
    df.createOrReplaceTempView("us_delay_flights_tbl")




    println("Find all flights whose distance is greater than 1,000 miles")
    df
      .select("distance", "origin", "destination")
      .where(col("distance")> 1000)
      .orderBy(desc("distance"))
      .show(10, false)


    println("Find all flights between San Francisco (SFO) and Chicago(ORD) with at least a two-hour delay:")
    spark.sql("""SELECT date, delay, origin, destination
                FROM us_delay_flights_tbl
                WHERE delay > 120 AND ORIGIN = 'SFO' AND DESTINATION = 'ORD'
                ORDER by delay DESC""").show(10)

    df.printSchema()

    val formato = df.withColumn("Date", to_timestamp(col("date"), "MMddhhmm"))

    formato.show()

    println("Find the months when these delays were most common")
    formato
      .select(month(col("Date")).alias("Date"), col("delay"))
      .distinct()
      .groupBy("Date")
      .agg(count("delay").alias("Cantidad"))
      .orderBy(desc("Cantidad"))
      .show(10, false)


    spark.sql("""SELECT delay, origin, destination,
       CASE
       WHEN delay > 360 THEN 'Very Long Delays'
       WHEN delay > 120 AND delay < 360 THEN 'Long Delays'
       WHEN delay > 60 AND delay < 120 THEN 'Short Delays'
       WHEN delay > 0 and delay < 60 THEN 'Tolerable Delays'
       WHEN delay = 0 THEN 'No Delays'
       ELSE 'Early'
       END AS Flight_Delays
       FROM us_delay_flights_tbl
        ORDER BY origin, delay DESC""").show(10)

    CREATE OR REPLACE GLOBAL TEMP VIEW us_origin_airport_SFO_global_tmp_view AS
      SELECT date, delay, origin, destination from us_delay_flights_tbl WHERE
      origin = 'SFO';
    CREATE OR REPLACE TEMP VIEW us_origin_airport_JFK_tmp_view AS
    SELECT date, delay, origin, destination from us_delay_flights_tbl WHERE
      origin = 'JFK'
  }
}
