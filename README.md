scalatest stream-specs
----------------------

- setup auth profile in `application.properties`

| strategy           | kinesis               | kafka     |
|--------------------|-----------------------|-----------|
| earliest           | TRIM_HORIZON          | earliest  |
| latest             | LATEST                | latest    |
| at_event_offset    | AT_SEQUENCE_NUMBER    |           |
| after_event_offset | AFTER_SEQUENCE_NUMBER |           |
| at_timestamp       | AT_TIMESTAMP          |           |

KinesisStream usage

```scala
class KinesisEmbeddedStreamSpecs extends FunSuite with BeforeAndAfterEach with Mathcers {

  val eventStream = new KinesisEmbeddedStream

  implicit val streamConfig = StreamConfig(stream = "TestStream", numOfPartition = 1)

  var partitionId = ""

  override protected def beforeEach(): Unit = {
    partitionId = eventStream.startBroker._2.head
  }
  
  override protected def afterEach(): Unit = eventStream.destroyBroker

  test("appends and consumes an event") {

    implicit val consumerConfig = ConsumerConfig(name = "TestStreamConsumer", partitionId = partitionId, strategy = "earliest")
    
    eventStream.appendEvent("TestStream", """{"eventId" : "uniqueId", "data" : "something-secret"}""".stripMargin)

    Thread.sleep(1500)

    eventStream.consumeEvent(streamConfig, consumerConfig, streamConfig.stream).size shouldBe 1
  }

}

```
