scalatest-eventstream
---------------------

- This project is a scalatest API for integration/component testing 
Streaming architecture.

- Currently supports two popular eventstores **KafkaStream** and **KinesisStream**.

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

check ports
---

```bash
λ netstat -na | grep 9092
tcp4      21      0  127.0.0.1.9092         127.0.0.1.63067        CLOSE_WAIT 
tcp4       0      0  127.0.0.1.63067        127.0.0.1.9092         FIN_WAIT_2 
tcp4      24      0  127.0.0.1.9092         127.0.0.1.62984        CLOSE_WAIT 
tcp4      24      0  127.0.0.1.9092         127.0.0.1.62975        CLOSE_WAIT 
tcp4       0      0  127.0.0.1.9092         127.0.0.1.62958        ESTABLISHED
tcp4       0      0  127.0.0.1.62958        127.0.0.1.9092         ESTABLISHED
tcp4       0      0  127.0.0.1.9092         127.0.0.1.62957        ESTABLISHED
tcp4       0      0  127.0.0.1.62957        127.0.0.1.9092         ESTABLISHED
tcp4       0      0  127.0.0.1.9092         127.0.0.1.62953        ESTABLISHED
tcp4       0      0  127.0.0.1.62953        127.0.0.1.9092         ESTABLISHED
tcp4       0      0  127.0.0.1.9092         *.*                    LISTEN  

λ netstat -na | grep 2181
tcp4      49      0  127.0.0.1.2181         127.0.0.1.63202        CLOSE_WAIT 
tcp4       0      0  127.0.0.1.63202        127.0.0.1.2181         FIN_WAIT_2 
tcp4      49      0  127.0.0.1.2181         127.0.0.1.63041        CLOSE_WAIT 
tcp4       0      0  127.0.0.1.63041        127.0.0.1.2181         FIN_WAIT_2 
tcp6      49      0  ::1.2181               ::1.63034              CLOSE_WAIT 
tcp6       0      0  ::1.63034              ::1.2181               FIN_WAIT_2 
tcp6       0      0  ::1.2181               ::1.62955              ESTABLISHED
tcp6       0      0  ::1.62955              ::1.2181               ESTABLISHED
tcp6       0      0  ::1.2181               ::1.62954              ESTABLISHED
tcp6       0      0  ::1.62954              ::1.2181               ESTABLISHED
tcp4       0      0  127.0.0.1.2181         127.0.0.1.62952        ESTABLISHED
tcp4       0      0  127.0.0.1.62952        127.0.0.1.2181         ESTABLISHED
tcp46      0      0  *.2181                 *.*                    LISTEN


kafka_2.12-1.1.0/bin/kafka-topics.sh --zookeeper localhost:2181 --list
[2019-05-06 19:53:54,310] WARN Client session timed out, have not heard from server in 15003ms for sessionid 0x0 (org.apache.zookeeper.ClientCnxn)
Exception in thread "main" kafka.zookeeper.ZooKeeperClientTimeoutException: Timed out waiting for connection while in state: CONNECTING
	at kafka.zookeeper.ZooKeeperClient.$anonfun$waitUntilConnected$3(ZooKeeperClient.scala:225)
	at scala.runtime.java8.JFunction0$mcV$sp.apply(JFunction0$mcV$sp.java:12)
	at kafka.utils.CoreUtils$.inLock(CoreUtils.scala:250)
	at kafka.zookeeper.ZooKeeperClient.waitUntilConnected(ZooKeeperClient.scala:221)
	at kafka.zookeeper.ZooKeeperClient.<init>(ZooKeeperClient.scala:95)
	at kafka.zk.KafkaZkClient$.apply(KafkaZkClient.scala:1539)
	at kafka.admin.TopicCommand$.main(TopicCommand.scala:57)
	at kafka.admin.TopicCommand.main(TopicCommand.scala)
```

how to use it
-------------

build it

```bash
git clone https://github.com/duwamish-os/scalatest-eventstream.git
cd scalatest-eventstream

# test against Kafka by default
mvn clean install
#to test against Kinesis, change tags in pom.xml
<tagsToInclude>KinesisStream</tagsToInclude>

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

TODO
----
- upgrade kafka from 1.1.0 to 2.2.x
- upgrade kinesis client to 2.2