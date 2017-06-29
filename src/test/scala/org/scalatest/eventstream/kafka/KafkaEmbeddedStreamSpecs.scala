package org.scalatest.eventstream.kafka

import java.util.Properties

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.scalatest.{BeforeAndAfterAll, FunSpec, Matchers}
import org.scalatest.eventstream.StreamConfig
import org.scalatest.eventstream.tags.KafkaStreamTag

/**
  * Created by prayagupd
  * on 6/15/17.
  */

class KafkaEmbeddedStreamSpecs extends FunSpec with BeforeAndAfterAll with Matchers {

  implicit val config =
    StreamConfig(streamTcpPort = 9092, streamStateTcpPort = 2181, stream = "gregor-samsa-for-no-use", numOfPartition = 1)

  val kafkaStream = new KafkaEmbeddedStream

  override protected def beforeAll(): Unit = {
    kafkaStream.startBroker
  }

  override protected def afterAll(): Unit = {
    kafkaStream.destroyBroker
  }

  describe("Kafka Embedded stream") {
    it("does consume earliest events", KafkaStreamTag) {

      //uses application.properties
      //emitter.broker.endpoint=localhost:9092
      //emitter.event.key.serializer=org.apache.kafka.common.serialization.StringSerializer
      //emitter.event.value.serializer=org.apache.kafka.common.serialization.StringSerializer

      kafkaStream.appendEvent("gregor-samsa-stream-earliest", """{"MyEvent" : { "myKey" : "myValue"}}""")

      val consumerProperties = new Properties()
      consumerProperties.put("bootstrap.servers", "localhost:9092")
      consumerProperties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
      consumerProperties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
      consumerProperties.put("group.id", "something_earliest")
      consumerProperties.put("auto.offset.reset", "earliest")

      val myConsumer = new KafkaConsumer[String, String](consumerProperties)
      myConsumer.subscribe(java.util.Collections.singletonList("gregor-samsa-stream-earliest"))

      val events = myConsumer.poll(2000)

      events.count() shouldBe 1
      events.iterator().next().value() shouldBe """{"MyEvent" : { "myKey" : "myValue"}}"""
    }

    it("does consume latest events", KafkaStreamTag) {

      val consumerProperties = new Properties()
      consumerProperties.put("bootstrap.servers", "localhost:9092")
      consumerProperties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
      consumerProperties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
      consumerProperties.put("group.id", "something_earliest")
      consumerProperties.put("auto.offset.reset", "earliest")

      val myConsumer = new KafkaConsumer[String, String](consumerProperties)
      myConsumer.subscribe(java.util.Collections.singletonList("gregor-samsa-stream-latest"))

      val eventsBeforeEmit = myConsumer.poll(2000)
      myConsumer.poll(2000).count() shouldBe 0

      //uses application.properties
      //emitter.broker.endpoint=localhost:9092
      //emitter.event.key.serializer=org.apache.kafka.common.serialization.StringSerializer
      //emitter.event.value.serializer=org.apache.kafka.common.serialization.StringSerializer
      kafkaStream.appendEvent("gregor-samsa-stream-latest", """{"MyEvent" : { "myKey" : "myValue"}}""")

      val events = myConsumer.poll(2000)

      events.count() shouldBe 1
      events.iterator().next().value() shouldBe """{"MyEvent" : { "myKey" : "myValue"}}"""
    }
  }
}
