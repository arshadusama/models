
import io.circe.generic.auto._
import io.circe.syntax._
import models.RSVP
import org.apache.spark.sql.functions.col
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Dataset, SparkSession}
import service.{Application, DataCleaner, Logic, Util}

import scala.concurrent.duration.DurationInt
import org.elasticsearch.spark._
import org.elasticsearch.spark.sql.sparkDatasetFunctions

object Main {

  def sparkConf = new SparkConf().setAppName("Main").setMaster("local").set("es.index.auto.create", "true").set("es.nodes", "http://192.168.100.144").set("es.port", "9200")

  def main(args: Array[String]): Unit = {


    implicit val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate() // toread
    val fileName = "meetup.json" // to add in cmd
    val rsvpFilePath = getClass.getResource(fileName).getPath

    import sparkSession.implicits._ // to read
    val rawData = sparkSession.read.textFile(rsvpFilePath)

    val rsvpObjects = rawData.map(RSVP.toObject).filter(_.isDefined).map(_.get).collect.toList.asJson.toString

    val df = sparkSession.read.json(Seq(rsvpObjects).toDS)

    Application().start(df)
  }
}
