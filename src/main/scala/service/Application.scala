package service

import models.Result
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row, SparkSession, functions}
import org.apache.spark.sql.functions.{col, from_unixtime, window}
import org.elasticsearch.spark.sql.sparkDatasetFunctions

import java.sql.Timestamp
import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`

case class Application(){

  def yesResponse: Column = col("response") === "yes"
  def eventTimeNotNull: Column = col("event.time").isNotNull
  def filerDataFrame(df: DataFrame):Dataset[Row] = df.filter(eventTimeNotNull).filter(yesResponse)
  def start(df: DataFrame)(implicit sc: SparkSession): Unit ={

    import  sc.implicits._

    val filteredData = filerDataFrame(df)
    val citiesWithTime = filteredData.select("event.time","group.group_city")

    val data = citiesWithTime
      .select("*")
      .withColumn("time", from_unixtime(col("time")/1000))
      .withColumn("cities", col("group_city"))

    val res = data.select(col("time"),col("cities") ,col("group_city"))
      .groupBy(window(col("time"), "7 days", "7 days"), col("cities"))
      .agg(functions.count("group_city"))
      .orderBy(col("window"))

    println(s"schema of result is  ${res.schema}")

    val objects = res.map{ row =>

      val time = row.getList[Timestamp](0).toList
      val cityName = row.getString(1)
      val score = row.getInt(2)
      Result(time = time.head, cityName = cityName, score = score)
    }
    objects.saveToEs("rsvp/docs")

  }
}
