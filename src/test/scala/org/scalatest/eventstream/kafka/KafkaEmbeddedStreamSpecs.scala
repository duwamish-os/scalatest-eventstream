package org.scalatest.eventstream.kafka

import java.util.concurrent.atomic.AtomicBoolean
import java.util.{Properties, UUID}

import org.apache.kafka.clients.consumer.{ConsumerRecords, KafkaConsumer}
import org.scalatest.{BeforeAndAfterAll, FunSpec, Matchers}
import org.scalatest.eventstream.{ConsumerConfig, StreamConfig}
import org.scalatest.eventstream.tags.KafkaStreamTag

/**
  * author prayagupd
  * on 6/15/17.
  */

class KafkaEmbeddedStreamSpecs extends FunSpec with BeforeAndAfterAll with Matchers {
  private val TestStream = "test-topic"
  implicit val config = StreamConfig(
    streamTcpPort = 9092,
    streamStateTcpPort = 2181,
    stream = TestStream,
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
      kafkaStream.appendEvent(TestStream, """{"MyEvent" : { "myKey" : "myValue"}}""")

      val consumerProperties = new Properties()
      consumerProperties.put("bootstrap.servers", "localhost:9092")
      //consumerProperties.put("client.id", s"something_client_${UUID.randomUUID().toString}")
      consumerProperties.put("group.id", s"something_group_${UUID.randomUUID().toString}")
      consumerProperties.put("auto.offset.reset", "earliest")
      consumerProperties.put("max.poll.records", "100")
      consumerProperties.put("enable.auto.commit", "true")
      consumerProperties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
      consumerProperties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")

      val myConsumer = new KafkaConsumer[String, String](consumerProperties)
      myConsumer.subscribe(java.util.Collections.singletonList(TestStream))

      var events: ConsumerRecords[String, String] = null
      val keepProcessing = new AtomicBoolean(true)
      var counter = 0
      while(keepProcessing.get() && counter < 100) {
        events = myConsumer.poll(2000)
        if(events.count() > 0) {
          keepProcessing.set(false)
        }
        counter = counter + 1
      }

      println(events)
      Option(events).map(_.iterator().next().value()) shouldBe Some("""{"MyEvent" : { "myKey" : "myValue"}}""")
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
