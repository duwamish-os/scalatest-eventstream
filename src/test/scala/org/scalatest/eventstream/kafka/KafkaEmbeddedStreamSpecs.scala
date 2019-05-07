package org.scalatest.eventstream.kafka

import java.util.Properties

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.scalatest.{BeforeAndAfterAll, FunSpec, Matchers}
import org.scalatest.eventstream.StreamConfig
import org.scalatest.eventstream.tags.KafkaStreamTag

/**
  * author prayagupd
  * on 6/15/17.
  */

class KafkaEmbeddedStreamSpecs extends FunSpec with BeforeAndAfterAll with Matchers {
  implicit val config = StreamConfig(
    streamTcpPort = 9092,
    streamStateTcpPort = 2181,
    stream = "test-topic",
    numOfPartition = 1
  )

  val kafkaStream = new KafkaEmbeddedStream

  override protected def beforeAll(): Unit = {
    kafkaStream.startBroker
  }

  override protected def afterAll(): Unit = {
    kafkaStream.destroyBroker
  }

  describe("Kafka Embedded stream") {
    it("does consume some events", KafkaStreamTag) {

      //uses application.properties
      //emitter.broker.endpoint=localhost:9092
      //emitter.event.key.serializer=org.apache.kafka.common.serialization.StringSerializer
      //emitter.event.value.serializer=org.apache.kafka.common.serialization.StringSerializer

      kafkaStream.appendEvent("test-topic", """{"MyEvent" : { "myKey" : "myValue"}}""")

      val consumerProperties = new Properties()
      consumerProperties.put("bootstrap.servers", "localhost:9092")
      consumerProperties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
      consumerProperties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
      consumerProperties.put("group.id", "something")
      consumerProperties.put("auto.offset.reset", "earliest")
//      consumerProperties.put("offsets.topic.replication.factor", "1")
//      consumerProperties.put("transaction.state.log.replication.factor", "1")
//      consumerProperties.put("transaction.state.log.min.isr", "1")

      val myConsumer = new KafkaConsumer[String, String](consumerProperties)
      myConsumer.subscribe(java.util.Collections.singletonList("test-topic"))

      val events = myConsumer.poll(2000)

      events.iterator().next().value() shouldBe """{"MyEvent" : { "myKey" : "myValue"}}"""
    }

    it("does consume latest events", KafkaStreamTag) {

      val consumerProperties = new Properties()
      consumerProperties.put("bootstrap.servers", "localhost:9092")
      consumerProperties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
      consumerProperties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
      consumerProperties.put("group.id", "something")
      consumerProperties.put("auto.offset.reset", "latest")

      val myConsumer = new KafkaConsumer[String, String](consumerProperties)
      myConsumer.subscribe(java.util.Collections.singletonList("latest-test-stream"))

      val eventsBeforeEmit = myConsumer.poll(2000)
      myConsumer.poll(2000).count() shouldBe 0

      //uses application.properties
      //emitter.broker.endpoint=localhost:9092
      //emitter.event.key.serializer=org.apache.kafka.common.serialization.StringSerializer
      //emitter.event.value.serializer=org.apache.kafka.common.serialization.StringSerializer
      kafkaStream.appendEvent("latest-test-stream", """{"MyEvent" : { "myKey" : "myValue"}}""")

      val events = myConsumer.poll(2000)

      events.count() shouldBe 1
      events.iterator().next().value() shouldBe """{"MyEvent" : { "myKey" : "myValue"}}"""
    }
  }
}
