package org.scalatest.eventstream.kafka

import java.net.InetSocketAddress
import java.time.{LocalDateTime, ZonedDateTime}
import java.util
import java.util.{Collections, Date, Properties, UUID}

import kafka.admin.{AdminUtils, RackAwareMode}
import kafka.server.{KafkaConfig, KafkaServer}
import kafka.utils.{StateServer, ZkUtils}
import org.I0Itec.zkclient.{ZkClient, ZkConnection}
import org.apache.kafka.clients.consumer.{ConsumerRecords, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
//import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.protocol.SecurityProtocol
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.zookeeper.server.{ServerCnxnFactory, ZooKeeperServer}
import org.json.JSONObject
import org.scalatest.eventstream.events.Event
import org.scalatest.eventstream.{Config, ConsumerConfig, EmbeddedStream, StreamConfig}

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.reflect.io.Directory

/**
  * Embedded eventstream for kafka
  *
  * author prayagupd
  * on 2/9/17.
  */

class KafkaEmbeddedStream extends EmbeddedStream {

  val config = Config.getConfig

  private[this] var stateFactory: Option[ServerCnxnFactory] = None
  private[this] var brokers: Option[KafkaServer] = None
  private[this] val logsDirs = mutable.Buffer.empty[Directory]

  val emitterConfig = new util.HashMap[String, String]() {
    {
      put("emitter.broker.endpoint", "bootstrap.servers")
      put("emitter.event.key.serializer", "key.serializer")
      put("emitter.event.value.serializer", "value.serializer")
      //      put("broker.offsets.topic.replication.factor", "offsets.topic.replication.factor")
      //      put("broker.transaction.state.log.replication.factor", "transaction.state.log.replication.factor")
      //      put("broker.transaction.state.log.min.isr", "transaction.state.log.min.isr")
    }
  }

  override def startBroker(implicit streamConfig: StreamConfig): (String, List[String], String) = {

    val seconds = ZonedDateTime.now().toEpochSecond
    val stateLogsDir = Directory.makeTemp(s"stream-state-logs-$seconds")
    val streamBrokerLogsDir = Directory.makeTemp(s"stream-broker-logs-$seconds")

    stateFactory = startZooKeeper(streamConfig.streamStateTcpPort, stateLogsDir)
    brokers = Option(startKafkaBroker(streamConfig, streamBrokerLogsDir))

    logsDirs ++= Seq(stateLogsDir, streamBrokerLogsDir)

    println(s"creating stream ${streamConfig.stream} after setup : ${String.join(",", listStreams(streamConfig))}")

    createStreamAndWait(streamConfig.stream, streamConfig.numOfPartition)

    (streamConfig.stream, List("0"), "ACTIVE") //FIXME partitions, responds 0 as default now
  }

  override def destroyBroker(implicit streamConfig: StreamConfig): Unit = {
    stopStreamBroker()
    stopStreamState()
    logsDirs.foreach(_.deleteRecursively())
    logsDirs.clear()
  }

  override def appendEvent(stream: String, event: String): Event = {
    println(s"===========================================")
    println(s"emitting an event ${event} to ${stream}")
    println(s"===========================================")

    val map = new mutable.HashMap[String, String]()

    new Properties() {
      {
        val resource = classOf[KafkaEmbeddedStream].getClassLoader.getResourceAsStream(config)
        println("==========================================")
        println("resource: " + resource)
        println("==========================================")
        load(resource)
      }
    }.entrySet().toSet.filter(kv => kv.getKey.toString.startsWith("emitter."))
      .foreach(entry => map.put(emitterConfig.get(entry.getKey.toString), entry.getValue.toString))

    val kafkaProducer = new KafkaProducer[String, String](map)
    val response = kafkaProducer.send(new ProducerRecord[String, String](stream, event))
    val r = response.get()

    val p = Event(
      r.offset() + "",
      r.checksum(),
      r.partition() + ""
    )

    println(s"emitted an event $p to $stream")

    p
  }

  override def consumeEvent(implicit streamConfig: StreamConfig,
                            consumerConfig: ConsumerConfig,
                            stream: String): List[JSONObject] = {
    val nativeKafkaConsumer = new KafkaConsumer[String, String](new Properties() {
      {
        put("bootstrap.servers", s"localhost:${streamConfig.streamTcpPort}")
        put("client.id", s"${consumerConfig.name}")
        put("group.id", s"${consumerConfig.name}_group_${UUID.randomUUID()}")
        put("auto.offset.reset", s"${consumerConfig.strategy}")
        put("key.deserializer", classOf[StringDeserializer].getName)
        put("value.deserializer", classOf[StringDeserializer].getName)
      }
    })

    nativeKafkaConsumer.subscribe(Collections.singletonList(stream))

    assertStreamExists(streamConfig)

    println(s"Consuming stream ${streamConfig.stream}")

    val events: ConsumerRecords[String, String] = nativeKafkaConsumer.poll(1000)

    nativeKafkaConsumer.wakeup()

    events.map(j => new JSONObject(j.value())).toList
  }

  override def assertStreamExists(streamConfig: StreamConfig): Unit = {
    val streamExists = AdminUtils.topicExists(new ZkUtils(new ZkClient(s"localhost:${streamConfig.streamStateTcpPort}",
      10000, 15000), new ZkConnection(s"localhost:${streamConfig.streamStateTcpPort}"), false), streamConfig.stream)

    if (!streamExists) {
      println("KafkaStreams : " + listStreams(streamConfig).mkString(","))
    }

    assert(streamExists)
  }

  private val LocalZooServer = "localhost:2181"

  override def createStreamAndWait(stream: String, partition: Int): (String, List[String], String) = {
    println(s"creating stream ${stream}://partition${partition}")

    if (describeStream(stream)) {
      println(s"stream $stream exists")

      return (stream, List("0"), "ACTIVE")
    }

    val stateConnection = StateServer.createConnection(LocalZooServer)

    AdminUtils.createTopic(stateConnection._2, stream, partition, replicationFactor = 1, new Properties() {},
      RackAwareMode.Enforced)

    stateConnection._1.close()

    println(s"stream created ${stream}://${partition}")

    (stream, List("0"), "ACTIVE")
  }

  def describeStream(stream: String): Boolean = {
    val streamExists = AdminUtils.topicExists(
      new ZkUtils(new ZkClient(s"$LocalZooServer", 10000, 15000),
        new ZkConnection(s"$LocalZooServer"), false),
      stream
    )
    streamExists
  }

  def startZooKeeper(zooKeeperPort: Int, zkLogsDir: Directory): Option[ServerCnxnFactory] = {
    val tickTime = 2000

    val zkServer = new ZooKeeperServer(zkLogsDir.toFile.jfile, zkLogsDir.toFile.jfile, tickTime)

    val cnxnFactory = ServerCnxnFactory.createFactory
    cnxnFactory.configure(new InetSocketAddress("0.0.0.0", zooKeeperPort), 1024)
    cnxnFactory.startup(zkServer)

    println(s"Stream-state-server started ${cnxnFactory.getLocalAddress.getHostName}:${cnxnFactory.getLocalPort} - at ${zkLogsDir.toFile}")
    Option(cnxnFactory)
  }

  def startKafkaBroker(config: StreamConfig,
                       kafkaLogDir: Directory): KafkaServer = {

    val syncServiceAddress = s"localhost:${config.streamStateTcpPort}"
    val listener = s"${SecurityProtocol.PLAINTEXT}://localhost:${config.streamTcpPort}"

    //kafka.server

    val properties: Properties = new Properties
    properties.setProperty("zookeeper.connect", syncServiceAddress)
    properties.setProperty("broker.id", "0")
    properties.setProperty("listeners", listener)
    properties.setProperty("advertised.listeners", listener)
    properties.setProperty("host.name", "localhost")
    properties.setProperty("advertised.host.name", "localhost")
    properties.setProperty("port", config.streamTcpPort.toString)
    properties.setProperty("auto.create.topics.enable", "true")
    properties.setProperty("log.flush.interval.messages", "1")
    properties.setProperty("log.dir", kafkaLogDir.toAbsolute.path)
    properties.setProperty("log.flush.interval.messages", 1.toString)
    properties.setProperty("offsets.topic.replication.factor", 1.toString)
    properties.setProperty("offsets.topic.num.partitions", 1.toString)
    properties.setProperty("transaction.state.log.replication.factor", "1")
    properties.setProperty("transaction.state.log.min.isr", "1")

    // The total memory used for log de-duplication across all cleaner threads,
    // keep it small to not exhaust suite memory
    properties.setProperty("log.cleaner.dedupe.buffer.size", "1048577")

    config.nodes.foreach { case (key, value) => properties.setProperty(key, value) }

    val broker = new KafkaServer(new KafkaConfig(properties))
    broker.startup()

    println(s"KafkaStream Broker started at ${properties.get("host.name")}:${properties.get("port")} at ${kafkaLogDir.toFile}")
    broker
  }

  override def listStreams(implicit streamConfig: StreamConfig): List[String] = {
    val zk = new ZkUtils(new ZkClient(s"localhost:${streamConfig.streamStateTcpPort}",
      10000, 15000), new ZkConnection(s"localhost:${streamConfig.streamStateTcpPort}"), false)

    AdminUtils.fetchAllTopicConfigs(zk).map(_._1).toList
  }

  def stopStreamBroker() = {
    brokers.foreach { b =>
      b.shutdown()
      b.awaitShutdown()
    }
    brokers = None
  }

  def stopStreamState(): Unit = {
    stateFactory.foreach(_.shutdown())
    stateFactory = None
  }

  override def dropConsumerState(stateTable: String): String = "" //FIXME
}
