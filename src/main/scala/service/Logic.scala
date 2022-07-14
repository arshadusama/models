package service

import models.RSVP

case class Logic() {
  def getYesResponse(rsvp: RSVP): Boolean = rsvp.response.toLowerCase == "yes"
}
