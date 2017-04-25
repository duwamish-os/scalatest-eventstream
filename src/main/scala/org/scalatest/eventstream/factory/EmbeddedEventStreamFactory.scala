package org.scalatest.eventstream.factory

import com.typesafe.config.ConfigFactory
import org.scalatest.eventstream.{Config, EmbeddedStream}
import org.scalatest.eventstream.kafka.KafkaEmbeddedStream
import org.scalatest.eventstream.kinesis.KinesisEmbeddedStream

import scala.collection.JavaConversions._

/**
  * Created by prayagupd
  * on 2/10/17.
  */

class EmbeddedEventStreamFactory {
  def create(): EmbeddedStream = {
    val appConfig = ConfigFactory.load(Config.getConfig)

    println("config keys - ")

    appConfig.entrySet().foreach({ x =>
      //println(x)
    })

    appConfig.getString("stream.driver") match {
      case "Kafka" => new KafkaEmbeddedStream
      case "Kinesis" => new KinesisEmbeddedStream
      case _ => throw new RuntimeException("You forgot to configure the stream driver.")
    }
  }
}
