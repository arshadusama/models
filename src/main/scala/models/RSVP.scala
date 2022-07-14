package models

import io.circe.generic.auto._
import io.circe.parser._

case class RSVP(venue: Option[Venue], visibility: String, response: String, guests: Int, member: Option[Member], rsvp_id: Int, mtime: Long, event: Event, group: Group)

object RSVP {
  def toObject(json: String): Option[RSVP] = decode[RSVP](json).toOption
}
