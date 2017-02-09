package org.scalatest.eventstream

import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}

/**
  * Created by prayagupd
  * on 2/9/17.
  */

case class EventStreamConfig(eventStreamPort: Int = 9092, eventStreamStatePort :Int = 2181,
                             nodes: Map[String, String] = Map.empty)

trait EmbeddedEventStream {
  def startBroker(implicit eventStreamConfig: EventStreamConfig)
  def stopBroker()
}

class KafkaEmbeddedEventStream extends EmbeddedEventStream {

  override def startBroker(implicit eventStreamConfig: EventStreamConfig): Unit = {
    implicit val x = EmbeddedKafkaConfig(eventStreamConfig.eventStreamPort, eventStreamConfig.eventStreamStatePort)
    EmbeddedKafka.start()
  }

  override def stopBroker(): Unit = EmbeddedKafka.stop()
}
