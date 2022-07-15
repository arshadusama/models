package service

import org.apache.spark.sql.{Column, DataFrame, Dataset, Row, functions}
import org.apache.spark.sql.functions.{col, from_unixtime, window}

case class Application(){

  def yesResponse: Column = col("response") === "yes"
  def eventTimeNotNull: Column = col("event.time").isNotNull
  def filerDataFrame(df: DataFrame):Dataset[Row] = df.filter(eventTimeNotNull).filter(yesResponse)
  def start(df: DataFrame): Unit ={
    val filteredData = filerDataFrame(df)
    val citiesWithTime = filteredData.select("event.time","group.group_city")

    val data = citiesWithTime
      .select("*")
      .withColumn("time", from_unixtime(col("time")/1000))

    val res = data.select("*")
      .groupBy(col("time"), window(col("time"), "12 hours", "12 hours"))
      .agg(functions.count("group_city"))

    res.show

//    data.withColumn("as_date", from_unixtime((col("time")/1000))).show()

//    val citiesByTimeGroup = citiesWithTime.groupBy(col("time"), window(col("time"), 604800000))
  }
}
