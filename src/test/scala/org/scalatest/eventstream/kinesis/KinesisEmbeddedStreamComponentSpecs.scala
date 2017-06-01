package org.scalatest.eventstream.kinesis

import java.util.Properties

import org.scalatest.eventstream.{ConsumerConfig, StreamConfig}
import org.scalatest.{BeforeAndAfterEach, FunSuite}

/**
  * Created by prayagupd
  * on 2/23/17.
  */

class KinesisEmbeddedStreamComponentSpecs extends FunSuite with BeforeAndAfterEach {

  val eventStream = new KinesisEmbeddedStream

  private val AppConfig = new Properties(){{
    load(this.getClass.getClassLoader.getResourceAsStream("application.properties"))
  }}

  val resourceIdentifier = AppConfig.getProperty("application.resource.identifier")

  implicit val streamConfig = StreamConfig(stream = s"${resourceIdentifier}-EmbeddedStream_Component", numOfPartition = 1)

  var partitionId = ""

  override protected def beforeEach(): Unit = {
    partitionId = eventStream.startBroker._2.head
  }

  override protected def afterEach(): Unit = eventStream.destroyBroker

  test("appends and consumes an event") {

    eventStream.appendEvent(
      s"${resourceIdentifier}-EmbeddedStream_Component", """{"eventId" : "uniqueId", "data" : "something-secret"}""".stripMargin)

    Thread.sleep(1500)

    implicit val consumerConfig = ConsumerConfig(name = "TestStreamConsumer", partitionId = partitionId, strategy = "earliest")
    assert(eventStream.consumeEvent(streamConfig, consumerConfig, streamConfig.stream).size == 1)
  }

  test("appends and consumes an event by eventStreamId") {

    val event2 = eventStream.appendEvent(
      s"${resourceIdentifier}-EmbeddedStream_Component", """{"eventId" : "uniqueId002", "data" : "something-very-secret"}""".stripMargin)

    val event1 = eventStream.appendEvent(
      s"${resourceIdentifier}-EmbeddedStream_Component", """{"eventId" : "uniqueId001", "data" : "something-super-secret"}""".stripMargin)

    Thread.sleep(1500)

    implicit val consumerConfig = ConsumerConfig(name = "TestStreamConsumer1", partitionId = partitionId, strategy = "at_event_offset")

    val events = eventStream.findEventByEventId(streamConfig, consumerConfig, streamConfig.stream, event2.offset)

    assert(events.size == 1)
    println("==========================")
    println(events.head)
    println("==========================")
  }

}
