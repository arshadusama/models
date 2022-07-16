
import io.circe.generic.auto._
import io.circe.syntax._
import models.Models.RSVP
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import service.Application

object Main {

  def sparkConf = new SparkConf()
    .setAppName("City-score")
    .setMaster("local")
    .set("es.index.auto.create", "true")
    .set("es.nodes", "http://192.168.100.144")
    .set("es.port", "9200")

  def main(args: Array[String]): Unit = {


    implicit val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    val fileName = "meetup.json"
    val rsvpFilePath = getClass.getResource(fileName).getPath

    import sparkSession.implicits._ // to read
    val rawData = sparkSession.read.textFile(rsvpFilePath)

    val rsvpObjects = rawData.map(RSVP.toObject).filter(_.isDefined).map(_.get).collect.toList.asJson.toString

    val df = sparkSession.read.json(Seq(rsvpObjects).toDS)

    Application().start(df)
  }
}
