package org.scalatest.eventstream.factory

import com.typesafe.config.ConfigFactory
import org.scalatest.eventstream.EmbeddedStream
import org.scalatest.eventstream.kafka.KafkaEmbeddedStream
import org.scalatest.eventstream.kinesis.KinesisEmbeddedStream

/**
  * Created by prayagupd
  * on 2/10/17.
  */

class EmbeddedEventStreamFactory {
  def create(): EmbeddedStream = {
    ConfigFactory.load("application.properties").getString("eventstream.driver") match {
      case "Kafka" => new KafkaEmbeddedStream
      case "Kinesis" => new KinesisEmbeddedStream
      case _ => throw new RuntimeException("You forgot to configure the stream driver.")
    }
  }
}
