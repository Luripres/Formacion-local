package org.prueba

import org.apache.spark
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.{col, countDistinct}
import org.apache.spark.sql.types.{BooleanType, FloatType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{functions => F}

object Firedp {
  def main(args: Array[String]) {

    val spark = SparkSession
      .builder.master("local[1]")
      .appName("Firedp")
      .getOrCreate()


    val fireSchema = StructType(Array(StructField("CallNumber", IntegerType, true),
                     StructField("UnitID", StringType, true), StructField("IncidentNumber", IntegerType, true),
                     StructField("CallType", StringType, true),
                     StructField("CallDate", StringType, true),
                     StructField("WatchDate", StringType, true),
                     StructField("CallFinalDisposition", StringType, true),
                     StructField("AvailableDtTm", StringType, true),
                     StructField("Address", StringType, true),
                     StructField("City", StringType, true),
                     StructField("Zipcode", IntegerType, true),
                     StructField("Battalion", StringType, true),
                     StructField("StationArea", StringType, true),
                     StructField("Box", StringType, true),
                     StructField("OriginalPriority", StringType, true),
                     StructField("Priority", StringType, true),
                     StructField("FinalPriority", IntegerType, true),
                     StructField("ALSUnit", BooleanType, true),
                     StructField("CallTypeGroup", StringType, true),
                     StructField("NumAlarms", IntegerType, true),
                     StructField("UnitType", StringType, true),
                     StructField("UnitSequenceInCallDispatch", IntegerType, true),
                     StructField("FirePreventionDistrict", StringType, true),
                     StructField("SupervisorDistrict", StringType, true),
                     StructField("Neighborhood", StringType, true),
                     StructField("Location", StringType, true),
                     StructField("RowID", StringType, true),
                     StructField("Delay", FloatType, true)))


    val sfFireFile="C:\\Users\\lucia.riera\\IdeaProjects\\Prueba\\data\\sf-fire-calls.csv"
    val fireDF = spark.read.schema(fireSchema)
      .option("header", "true")
      .csv(sfFireFile)


    // Guardado como archivo Parquet
    //val parquetPath = fireDF.write.format("parquet").save(parquetPath)

    // Guardado como tabla
    //val parquetTable = fireDF.write.format("parquet").saveAsTable(parquetTable)



    val fewFireDF = fireDF
      .select("IncidentNumber", "AvailableDtTm", "CallType")
      .where(col("CallType") =!= "Medical Incident")

    println("1")
    fewFireDF.show(5, false)

    println("2")
    fireDF
      .select("CallType")
      .where(col("CallType").isNotNull)
      .agg(countDistinct("CallType").alias("DistinctCallTypes"))
      .show()

    println("3")
    fireDF
      .select("CallType")
      .where(col("CallType").isNotNull)
      .distinct()
      .show(10, false)

    println("4")
    val newFireDF = fireDF.withColumnRenamed("Delay", "ResponseDelayedinMins")
    newFireDF
      .select("ResponseDelayedinMins")
      .where(col("ResponseDelayedinMins")> 5)
      .show(5, false)

    println("5")
    val fireTsDF = newFireDF
      .withColumn("IncidentDate", to_timestamp(col("CallDate"), "MM/dd/yyyy"))
      .drop("CallDate")
      .withColumn("OnWatchDate", to_timestamp(col("WatchDate"), "MM/dd/yyyy"))
      .drop("WatchDate")
      .withColumn("AvailableDtTS", to_timestamp(col("AvailableDtTm"),
        "MM/dd/yyyy hh:mm:ss a"))
      .drop("AvailableDtTm")
    // Select the converted columns

    println("6")
    fireTsDF
      .select("IncidentDate", "OnWatchDate", "AvailableDtTS")
      .show(5, false)

    println("7")
    fireTsDF
      .select(year(col("IncidentDate")))
      .distinct()
      .orderBy(year(col("IncidentDate")))
      .show()

    println("8")
    fireTsDF
      .select("CallType")
      .where(col("CallType").isNotNull)
      .groupBy("CallType")
      .count()
      .orderBy(desc("count"))
      .show(10, false)

    println("9")
    fireTsDF
      .select(F.sum("NumAlarms"), F.avg("ResponseDelayedinMins"),
        F.min("ResponseDelayedinMins"), F.max("ResponseDelayedinMins"))
      .show()



    println("What were all the different types of fire calls in 2018?")
    fireTsDF
      .select(col("CallType"))
      .distinct()
      .where(year(col("IncidentDate")) < 2019)
      //.where("Call type")
      .show()

    println("What months within the year 2018 saw the highest number of fire calls?")
    fireTsDF
      .select(col("IncidentDate"))
      .distinct()
      .where(year(col("IncidentDate")) < 2019)
      .groupBy(month(col("IncidentDate")))
      .count()
      .orderBy(desc("count"))
      .show(1)



    println("Which neighborhood in San Francisco generated the most fire calls in 2018?")
    fireTsDF
      .select(col("Neighborhood"))
      .where(year(col("IncidentDate")) < 2019)
      .groupBy(col("Neighborhood"))
      .count()
      .orderBy(desc("count"))
      .show(1)

    println("Which neighborhoods had the worst response times to fire calls in 2018?")
    fireTsDF
      .select(col("Neighborhood"), col("ResponseDelayedinMins"))
      .where(year(col("IncidentDate")) < 2019)
      //.groupBy(col("Neighborhood"))
      .orderBy(desc("ResponseDelayedinMins"))
      .show(1)
  }
}

