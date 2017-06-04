package org.scalatest.eventstream.kafka

import java.net.InetSocketAddress
import java.util.{Collections, Date, Properties, UUID}

import kafka.admin.{AdminUtils, RackAwareMode}
import kafka.server.{KafkaConfig, KafkaServer}
import kafka.utils.{StateServer, ZkUtils}
import net.manub.embeddedkafka.EmbeddedKafkaConfig
import org.I0Itec.zkclient.{ZkClient, ZkConnection}
import org.apache.kafka.clients.consumer.{ConsumerRecords, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.zookeeper.server.{ServerCnxnFactory, ZooKeeperServer}
import org.json.JSONObject
import org.scalatest.eventstream.events.Event
import org.scalatest.eventstream.{ConsumerConfig, EmbeddedStream, StreamConfig}

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.reflect.io.Directory

/**
  * Embedded eventstream for kafka
  *
  * Created by prayagupd
  * on 2/9/17.
  */

class KafkaEmbeddedStream extends EmbeddedStream {

  private[this] var stateFactory: Option[ServerCnxnFactory] = None
  private[this] var brokers: Option[KafkaServer] = None
  private[this] val logsDirs = mutable.Buffer.empty[Directory]

  override def startBroker(implicit streamConfig: StreamConfig): (String, List[String], String) = {

    val stateLogsDir = Directory.makeTemp(s"stream-state-logs-${new Date().getTime}")
    val brokerLogsDir = Directory.makeTemp(s"stream-logs-${new Date().getTime}")

    stateFactory = Option(startZooKeeper(streamConfig.streamStateTcpPort, stateLogsDir))
    brokers = Option(startKafkaBroker(streamConfig, brokerLogsDir))

    logsDirs ++= Seq(stateLogsDir, brokerLogsDir)

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
    val properties = new Properties() {{
      val resource = classOf[KafkaEmbeddedStream].getClassLoader.getResourceAsStream("producer.properties")
      println("==========================================")
      println(resource)
      println("==========================================")
      load(resource)
    }}

    val kafkaProducer = new KafkaProducer[String, String](properties)
    val response = kafkaProducer.send(new ProducerRecord[String, String](stream, event))

    Event(response.get().offset()+"", response.get().checksum(), response.get().partition()+"")
  }

  override def consumeEvent(implicit streamConfig: StreamConfig, consumerConfig: ConsumerConfig,
                            stream: String): List[JSONObject] = {
    val nativeKafkaConsumer = new KafkaConsumer[String, String](new Properties() {{
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

    events.map(x => new JSONObject(x.value())).toList
  }

  override def assertStreamExists(streamConfig: StreamConfig): Unit = {
    val streamExists = AdminUtils.topicExists(new ZkUtils(new ZkClient(s"localhost:${streamConfig.streamStateTcpPort}",
      10000, 15000), new ZkConnection(s"localhost:${streamConfig.streamStateTcpPort}"), false), streamConfig.stream)

    if(!streamExists) {
      println("KafkaStreams : " + listStreams(streamConfig).mkString(","))
    }

    assert(streamExists)
  }

  override def createStreamAndWait(stream: String, partition: Int): (String, List[String], String) = {

    val stateConnection = StateServer.createConnection("localhost:2181")

    val replicationFactor: Int = 1
    AdminUtils.createTopic(stateConnection._2, stream, partition, replicationFactor, new Properties() {},
      RackAwareMode.Enforced)

    stateConnection._1.close()

    (stream, List("0"), "ACTIVE")
  }

  def startZooKeeper(zkLogsDir: Directory)(
    implicit config: EmbeddedKafkaConfig): Unit = {
    stateFactory = Option(startZooKeeper(config.zooKeeperPort, zkLogsDir))
  }

  def startZooKeeper(zooKeeperPort: Int, zkLogsDir: Directory): ServerCnxnFactory = {
    val tickTime = 2000

    val zkServer = new ZooKeeperServer(zkLogsDir.toFile.jfile, zkLogsDir.toFile.jfile, tickTime)

    val factory = ServerCnxnFactory.createFactory
    factory.configure(new InetSocketAddress("0.0.0.0", zooKeeperPort), 1024)
    factory.startup(zkServer)
    factory
  }

  def startKafkaBroker(config: StreamConfig,
                       kafkaLogDir: Directory): KafkaServer = {

    val syncServiceAddress = s"localhost:${config.streamStateTcpPort}"

    val properties: Properties = new Properties
    properties.setProperty("zookeeper.connect", syncServiceAddress)
    properties.setProperty("broker.id", "0")
    properties.setProperty("host.name", "localhost")
    properties.setProperty("advertised.host.name", "localhost")
    properties.setProperty("port", config.streamTcpPort.toString)
    properties.setProperty("auto.create.topics.enable", "true")
    properties.setProperty("log.dir", kafkaLogDir.toAbsolute.path)
    properties.setProperty("log.flush.interval.messages", 1.toString)

    // The total memory used for log deduplication across all cleaner threads, keep it small to not exhaust suite memory
    properties.setProperty("log.cleaner.dedupe.buffer.size", "1048577")

    config.nodes.foreach {
      case (key, value) => properties.setProperty(key, value)
    }

    val broker = new KafkaServer(new KafkaConfig(properties))
    broker.startup()

    println(s"KafkaStream Broker started at ${properties.get("host.name")}:${properties.get("port")}")
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
