package service

import models.RSVP

case class DataCleaner() {

  private def venueExist(rsvp: RSVP): Boolean = rsvp.venue.isDefined

  private def eventTimeExist(rsvp: RSVP): Boolean = rsvp.event.time.isDefined

  def clean(rsvp: Option[RSVP]): Boolean = {
    rsvp match {
      case Some(rsvp) => venueExist(rsvp) && eventTimeExist(rsvp)
      case None => false
    }
  }
}
