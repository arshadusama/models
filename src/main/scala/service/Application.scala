package service

import models.Result
import io.circe.generic.auto._
import io.circe.syntax._
import io.circe.parser._
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row, SparkSession, functions}
import org.apache.spark.sql.functions._
import org.elasticsearch.spark.sql.sparkDatasetFunctions

import java.sql.Timestamp
import scala.util.parsing.json.JSONObject

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

    val cityCountsByTimeWindow = data.select(col("time"),col("cities") ,col("group_city"))
      .groupBy(window(col("time"), "1 days", "1 days"), col("cities"))
      .agg(functions.count("group_city").as("city_count"))
      .orderBy(col("window"))


    val cityCountsByTime = cityCountsByTimeWindow.map{ row =>

      val time = row.getStruct(0).getAs[Timestamp]("start")
      val cityName = row.getAs[String]("cities")
      val score = row.getAs[Long]("city_count")
      Result(time, cityName, score)
    }

    val trendingCities = cityCountsByTime.select(col("time"), col("cityName"), col("score"))
      .groupBy(col("time") )
      .agg(functions.max("score").as("score") )
      .orderBy("time")

    trendingCities.show()

    trendingCities.saveToEs("rsvp")

  }
}
