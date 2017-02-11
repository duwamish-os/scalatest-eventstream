package org.scalatest.eventstream.kafka

import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.scalatest.eventstream.EmbeddedEventStream

/**
  * Created by prayagupd
  * on 2/9/17.
  */

case class EventStreamConfig(eventStreamPort: Int = 9092, eventStreamStatePort :Int = 2181,
                             nodes: Map[String, String] = Map.empty)

class KafkaEmbeddedEventStream extends EmbeddedEventStream {

  override def startBroker(implicit eventStreamConfig: EventStreamConfig): Unit = {
    implicit val x = EmbeddedKafkaConfig(eventStreamConfig.eventStreamPort, eventStreamConfig.eventStreamStatePort)
    EmbeddedKafka.start()
  }

  override def stopBroker(): Unit = EmbeddedKafka.stop()
}
