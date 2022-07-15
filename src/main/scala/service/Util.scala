package service

import org.apache.spark.sql.SparkSession

case class Util()(implicit val sparkSession: SparkSession) {
  def toJson(filePath: String) : String = {

    val data = sparkSession.read.text(filePath)

    import sparkSession.implicits._
    val commaSeperatedObjects = data.map(_ + ",").collect.toList
    val tailObject = commaSeperatedObjects.lastOption.map(_.dropRight(1)).toList
    val result = commaSeperatedObjects.dropRight(1) ++ tailObject
    val concatString = result.fold("")( _ + _)
    "[\n" + concatString + "\n]"
  }
}
