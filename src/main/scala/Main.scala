import models.RSVP
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Dataset, SparkSession}
import service.{DataCleaner, Logic}

import scala.concurrent.duration.DurationInt
import org.elasticsearch.spark._
import org.elasticsearch.spark.sql.sparkDatasetFunctions

object Main {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("Main").setMaster("local").set("es.index.auto.create", "true")

    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate() // toread
    val fileName = "meetup.json" // to add in cmd
    val partitions = 1 // to add in cmd

    val rsvpFilePath = getClass.getResource(fileName).getPath

    import sparkSession.implicits._ // to read
    val rawData = sparkSession.read.textFile(rsvpFilePath)
    val repartitioned = rawData.repartition(partitions)

    val rsvpObjects = repartitioned.map(RSVP.toObject)
    val dataCleaner = DataCleaner()
    val rsvps = rsvpObjects.filter(rsvp => dataCleaner.clean(rsvp)).map(_.get)
    val logic = Logic()
    val membersJoiningEVent = rsvps.filter(rsvp => logic.getYesResponse(rsvp))

    membersJoiningEVent.saveToEs("spark/docs")


    val seperationTime = 1.days.toMillis


  }
}
