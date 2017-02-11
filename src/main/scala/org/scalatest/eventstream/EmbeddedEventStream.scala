package org.scalatest.eventstream

import org.scalatest.eventstream.kafka.EventStreamConfig

/**
  * Created by prayagupd
  * on 2/10/17.
  */

trait EmbeddedEventStream {
  def startBroker(implicit eventStreamConfig: EventStreamConfig)
  def stopBroker()
}
