package service

import models.Models.CityScore
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.elasticsearch.spark.sql.sparkDatasetFunctions

import java.sql.Timestamp

case class Application()(implicit sc: SparkSession) {

  import sc.implicits._

  def yesResponse: Column = $"response" === "yes"

  def eventTimeNotNull: Column = $"event.time".isNotNull

  def filerDataFrame(df: DataFrame): Dataset[Row] =
    df.filter(eventTimeNotNull).filter(yesResponse)

  def selectCitiesnEventTime(ds: Dataset[Row]) =
    ds.select("event.time", "group.group_city")

  def miliesToUnixTimeAndDuplicateCityColm(df: DataFrame) =
    df.select("*")
      .withColumn("time", from_unixtime($"time" / 1000))
      .withColumn("cities", $"group_city")

  def divideByTimeWindowAndCities(df: DataFrame) =
    df.select($"time", $"cities",$"group_city")
      .groupBy(window($"time", "1 days", "1 days"), $"cities")
      .agg(functions.count("group_city").as("city_count"))
      .orderBy($"window")

  def CityCountsByTimeWindow(ds: Dataset[Row]) =
    ds.map { row =>
      val time = row.getStruct(0).getAs[Timestamp]("start")
      val cityName = row.getAs[String]("cities")
      val score = row.getAs[Long]("city_count")
      CityScore(time, cityName, score)
    }

  def sortedTrendingEvents(ds: Dataset[CityScore]) =
    ds.select($"*")
      .groupBy("time")
      .agg(max($"score"))
      .orderBy("time")

  def trendingCityByEventTime(colRenameTrendingEventWithTime: DataFrame, cityCountsByTime: Dataset[CityScore]) =
    colRenameTrendingEventWithTime
      .join(cityCountsByTime, colRenameTrendingEventWithTime("event_time") === cityCountsByTime("time")
        && colRenameTrendingEventWithTime("count") === cityCountsByTime("score"),
        "inner"
      )
      .drop($"count")
      .drop($"event_time")

  def start(df: DataFrame): Unit = {

    /*
        remove data where event time is not present
        and response if 'no'
     */
    val filteredData = filerDataFrame(df)


    /*
        As, just cities and event_time is required, just select them
     */
    val citiesWithTime = selectCitiesnEventTime(filteredData)


    /*
        convert time in unix time and add just repeat group_city as cities
     */
    val cityWIthEVentTime = miliesToUnixTimeAndDuplicateCityColm(citiesWithTime)
    /*
    sample of above operation
    +-------------------+--------------+--------------+
    |               time|    group_city|        cities|
    +-------------------+--------------+--------------+
    |2017-03-25 02:00:00|    Hackensack|    Hackensack|
    |2017-03-19 17:30:00|        London|        London|
    |2017-04-09 07:00:00|         Tokyo|         Tokyo|
    |2017-03-31 22:30:00|        London|        London|
    |2017-03-25 02:00:00|    Hackensack|    Hackensack|
    |2017-03-25 04:00:00|        Nashua|        Nashua|
    |2017-03-20 03:00:00|     Annandale|     Annandale|
    |2017-03-20 22:30:00|       München|       München|
    |2017-03-22 03:00:00|Murrells Inlet|Murrells Inlet|
    |2017-03-19 18:00:00|        Berlin|        Berlin|
     */


    /*
        group by time window and city name, count their occurrence
     */
    val cityCountsByEventTimeWindow = divideByTimeWindowAndCities(cityWIthEVentTime)
    /*
    sample of above operation
    +------------------------------------------+------------+----------+
    |window                                    |cities      |city_count|
    +------------------------------------------+------------+----------+
    |{2017-03-19 05:00:00, 2017-03-20 05:00:00}|Clearwater  |1         |
    |{2017-03-19 05:00:00, 2017-03-20 05:00:00}|Edinburgh   |2         |
    |{2017-03-19 05:00:00, 2017-03-20 05:00:00}|Barcelona   |6         |
    |{2017-03-19 05:00:00, 2017-03-20 05:00:00}|Brussels    |3         |
    |{2017-03-19 05:00:00, 2017-03-20 05:00:00}|Hamburg     |1         |
    |{2017-03-19 05:00:00, 2017-03-20 05:00:00}|Philadelphia|1         |
    |{2017-03-19 05:00:00, 2017-03-20 05:00:00}|Ocean Grove |1         |
    |{2017-03-19 05:00:00, 2017-03-20 05:00:00}|Land O Lakes|1         |
    |{2017-03-19 05:00:00, 2017-03-20 05:00:00}|Saint Louis |3         |
    |{2017-03-19 05:00:00, 2017-03-20 05:00:00}|London      |11        |
    |{2017-03-19 05:00:00, 2017-03-20 05:00:00}|Toronto     |2         |
    |{2017-03-19 05:00:00, 2017-03-20 05:00:00}|Columbus    |1         |
     */


    /*
     as range is not required, just remove end time
     */
    val cityCountsByTime = CityCountsByTimeWindow(cityCountsByEventTimeWindow)
    /*
    sample of above operation
    +-------------------+------------+-----+
    |time               |cityName    |score|
    +-------------------+------------+-----+
    |2017-03-19 05:00:00|Philadelphia|1    |
    |2017-03-19 05:00:00|Paramus     |1    |
    |2017-03-19 05:00:00|Land O Lakes|1    |
    |2017-03-19 05:00:00|Lake Elmo   |2    |
    |2017-03-19 05:00:00|London      |11   |
    |2017-03-19 05:00:00|Quechee     |2    |
    |2017-03-19 05:00:00|Columbus    |1    |
    |2017-03-19 05:00:00|Lake Worth  |2    |
    |2017-03-19 05:00:00|Bethesda    |1    |
    |2017-03-19 05:00:00|Berlin      |3    |
     */

    /*
    now again group by again with one day and find max rsvps count
    city Name can't be selected as its not in group by and aggrigate
     */
    val trendingEventWithTime = sortedTrendingEvents(cityCountsByTime)
    /*
    sample of above operation
    +-------------------+----------+
    |time               |max(score)|
    +-------------------+----------+
    |2017-03-19 05:00:00|11        |
    |2017-03-20 05:00:00|10        |
    |2017-03-21 05:00:00|4         |
    |2017-03-22 05:00:00|4         |
    |2017-03-23 05:00:00|7         |
    |2017-03-24 05:00:00|5         |
    |2017-03-25 05:00:00|10        |
    |2017-03-26 05:00:00|4         |
     */


    /*
    rename col time with count and time event_time
     */
    val colRenameTrendingEventWithTime = trendingEventWithTime
      .select(col("max(score)").as("count"), col("time").as("event_time"))


    /*
    now join upbove cols to get city name as well
     */
    val trendingCitiesWithEventTime = trendingCityByEventTime(colRenameTrendingEventWithTime, cityCountsByTime)
    /*
    sample of above operation
    +-------------------+--------------------+-----+
    |               time|            cityName|score|
    +-------------------+--------------------+-----+
    |2017-03-19 05:00:00|              London|   11|
    |2017-03-20 05:00:00|              London|   10|
    |2017-03-21 05:00:00|              London|    4|
    |2017-03-21 05:00:00|               Dubai|    4|
    |2017-03-22 05:00:00|       Saint Charles|    4|
    |2017-03-23 05:00:00|           Amsterdam|    7|
    |2017-03-24 05:00:00|              London|    5|
    |2017-03-25 05:00:00|           Hong Kong|   10|
    |2017-03-26 05:00:00|               Paris|    4|
    |2017-03-26 05:00:00|             Bangkok|    4|
     */

    trendingCitiesWithEventTime.saveToEs("city-trends")

  }
}
