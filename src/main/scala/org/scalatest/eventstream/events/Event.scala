package org.scalatest.eventstream.events

/**
  * Created by prayagupd
  * on 3/7/17.
  */

case class Event(offset: String, checksum: Long, partition: String)
