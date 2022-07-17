import ApplicationSpec.{application, df}
import models.Models.RSVP
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.mockito.MockitoSugar
import org.scalatest.matchers.must.Matchers
import org.scalatest.wordspec.AnyWordSpec
import service.Application

class ApplicationSpec extends AnyWordSpec with Matchers with MockitoSugar {

  val filteredData = application.filerDataFrame(df)
  val selectedCities = application.selectCitiesnEventTime(filteredData)

  val dataWithTime  = application.miliesToUnixTimeAndDuplicateCityColm(selectedCities)

  "Spark Application" should {
    "filter data by response and event time" in {
      filteredData.count() mustBe 3
    }
    "should contain time and group city" in {
      selectedCities.columns.contains("event.time")  &&
        selectedCities.columns.contains("group.group_city") mustBe true
    }
    "should contain cities" in {
      dataWithTime.columns.contains("cities") mustBe true
    }
  }
}

object ApplicationSpec {
  def sparkConf = new SparkConf()
    .setAppName("City-score")
    .setMaster("local[*]")

  implicit val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

  import io.circe.generic.auto._
  import io.circe.syntax._
  import sparkSession.implicits._

  val file = getClass.getResource("sample.json").getFile

  val rawData = sparkSession.read.textFile(file)
  val rsvpObjects = rawData.map(RSVP.toObject).filter(_.isDefined).map(_.get).collect.toList.asJson.toString
  val df = sparkSession.read.json(Seq(rsvpObjects).toDS)
  val application = Application()
}
