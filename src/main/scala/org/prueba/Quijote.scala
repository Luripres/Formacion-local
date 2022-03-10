package org.prueba

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object Quijote {

  def main(args : Array[String]) {



    val spark:SparkSession = SparkSession.builder().master("local[1]")
      .appName("quijote")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val texto = spark.sparkContext.textFile("C:\\Datos\\quijote.txt")
    val num = texto.count()
    val prim = texto.first()
    val cinco = texto.take(5)
    println("El quijote tiene " + num + " líneas, y empieza:")
    println(prim)
    println(" ")
    println("Las primeras 5 líneas:")
    cinco.foreach(x=>println(x))

  }

}
