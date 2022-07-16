package models

import io.circe.generic.auto._
import io.circe.parser._

import java.sql.Timestamp

object Models {

  case class Event(event_name: String, event_id: String, time: Option[Long], event_url: String)

  case class Group(
                    group_topics: List[GroupTopics],
                    group_city: String,
                    group_state: Option[String],
                    group_country: String,
                    group_id: Int,
                    group_name: String,
                    group_lon: Double,
                    group_urlname: String,
                    group_lat: Double)

  case class GroupTopics(urlkey: String, topic_name: String)


  case class Member(member_id: Int, photo: Option[String], member_name: String)

  case class CityScore(time: Timestamp, cityName: String, score: Long)

  case class Venue(venue_name: String, lon: Double, lat: Double, venue_id: Int)

  case class RSVP(
                   venue: Option[Venue],
                   visibility: String,
                   response: String,
                   guests: Int,
                   member: Option[Member],
                   rsvp_id: Int,
                   mtime: Long,
                   event: Event,
                   group: Group
                 )

  object RSVP {
    def toObject(json: String): Option[RSVP] = decode[RSVP](json).toOption
  }
}
