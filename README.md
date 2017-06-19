scalatest-eventstream
---------------------

- This project is a scalatest API for integration/component testing 
Streaming architecture.

- Currently supports two popular eventstores KafkaStream and KinesisStream.

- For KinesisStream, it uses AWS Kinesis, as kinesis can not be run in local, so you 
need to setup AWS auth profile in `application.properties` so that you can emit 
or consume events from KinesisStream. In that sense KinesisStream is Pseudo-Embedded.

- Stream can be configured with a config `stream.driver` (`"Kafka"` or `"Kinesis"`).

Emitter configs
---------------

```
stream.driver
emitter.broker.endpoint
emitter.event.key.serializer
emitter.event.value.serializer
```

Consumer strategies
-------------------

| strategy           | kafka     | kinesis               |
|--------------------|-----------|-----------------------|
| earliest           | earliest  | TRIM_HORIZON          |
| latest             | latest    | LATEST                |
| at_event_offset    |           | AT_SEQUENCE_NUMBER    |
| after_event_offset |           | AFTER_SEQUENCE_NUMBER |
| at_timestamp       |           | AT_TIMESTAMP          |

KafkaStream usage
-----------------

```scala
class MyKafkaStreamConsumerSpecs extends FunSpec with BeforeAndAfterAll with Matchers {
  implicit val config =
    StreamConfig(streamTcpPort = 9092, streamStateTcpPort = 2181, stream = "test-topic", numOfPartition = 1)

  val kafkaStream = new KafkaEmbeddedStream

  override protected def beforeAll(): Unit = {
    kafkaStream.startBroker
  }

  override protected def afterAll(): Unit = {
    kafkaStream.destroyBroker
  }

  describe("Kafka Embedded stream") {
    it("does consume some events") {

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

      val myConsumer = new KafkaConsumer[String, String](consumerProperties)
      myConsumer.subscribe(java.util.Collections.singletonList("test-topic"))

      val events = myConsumer.poll(2000)

      events.count() shouldBe 1
      events.iterator().next().value() shouldBe """{"MyEvent" : { "myKey" : "myValue"}}"""
    }
  }
}

```

KinesisStream usage
-------------------

```scala
class MyKinesisStreamConsumerSpecs extends FunSuite with BeforeAndAfterEach with Mathcers {

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

how to use it
-------------

build it

```bash
git clone https://github.com/duwamish-os/scalatest-eventstream.git
cd scalatest-eventstream
mvn clean install
```

use it as maven dependency

```xml
<dependency>
  <groupId>org.scalatest.eventstream</groupId>
  <artifactId>scalatest-eventstream</artifactId>
  <version>1.0</version>
  <scope>test</scope>
</dependency>
```