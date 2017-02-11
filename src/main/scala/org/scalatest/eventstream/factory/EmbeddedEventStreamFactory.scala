package org.scalatest.eventstream.factory

import org.scalatest.eventstream.EmbeddedEventStream
import org.scalatest.eventstream.kafka.KafkaEmbeddedEventStream

/**
  * Created by prayagupd
  * on 2/10/17.
  */

class EmbeddedEventStreamFactory {
  def create(): EmbeddedEventStream = {
    new KafkaEmbeddedEventStream //match the driver in application.properties
  }
}
