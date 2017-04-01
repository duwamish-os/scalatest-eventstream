package org.scalatest.eventstream.kinesis

import org.scalatest.eventstream.{ConsumerConfig, StreamConfig}
import org.scalatest.{BeforeAndAfterEach, FunSuite}

/**
  * Created by prayagupd
  * on 2/23/17.
  */

class KinesisEmbeddedStreamComponentSpecs extends FunSuite with BeforeAndAfterEach {

  val eventStream = new KinesisEmbeddedStream

  implicit val streamConfig = StreamConfig(stream = "TestStream", numOfPartition = 1)

  var partitionId = ""

  override protected def beforeEach(): Unit = {
    partitionId = eventStream.startBroker._2.head
  }
  override protected def afterEach(): Unit = eventStream.destroyBroker

  test("appends and consumes an event") {

    eventStream.appendEvent("TestStream", """{"eventId" : "uniqueId", "data" : "something-secret"}""".stripMargin)

    Thread.sleep(1500)

    implicit val consumerConfig = ConsumerConfig(name = "TestStreamConsumer", partitionId = partitionId, strategy = "TRIM_HORIZON")
    assert(eventStream.consumeEvent(streamConfig, consumerConfig, streamConfig.stream).size == 1)
  }

}
