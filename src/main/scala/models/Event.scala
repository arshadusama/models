package models

case class Event(event_name: String, event_id: String, time: Option[Long], event_url: String)
