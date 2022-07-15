package models

import java.sql.Timestamp

case class Result(time: Timestamp, cityName: String, score: Long)
