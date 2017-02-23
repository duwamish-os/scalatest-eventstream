package org.scalatest.eventstream.kafka

import java.net.InetSocketAddress
import java.util.{Collections, Date, Properties}

import kafka.admin.AdminUtils
import kafka.server.{KafkaConfig, KafkaServer}
import kafka.utils.ZkUtils
import net.manub.embeddedkafka.EmbeddedKafkaConfig
import org.I0Itec.zkclient.{ZkClient, ZkConnection}
import org.apache.kafka.clients.consumer.{ConsumerRecords, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.zookeeper.server.{ServerCnxnFactory, ZooKeeperServer}
import org.json.JSONObject
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

  override def startBroker(implicit streamConfig: StreamConfig): Unit = {

    val stateLogsDir = Directory.makeTemp(s"state-logs-${new Date().getTime}")
    val brokerLogsDir = Directory.makeTemp(s"stream-logs-${new Date().getTime}")

    stateFactory = Option(startZooKeeper(streamConfig.streamStateTcpPort, stateLogsDir))
    brokers = Option(startKafkaBroker(streamConfig, brokerLogsDir))

    logsDirs ++= Seq(stateLogsDir, brokerLogsDir)

  }

  override def destroyBroker(implicit streamConfig: StreamConfig): Unit = {
    stopStreamBroker()
    stopStreamState()
    logsDirs.foreach(_.deleteRecursively())
    logsDirs.clear()
  }

  override def appendEvent(stream: String, event: String): (Long, Long, Int) = {
    val properties = new Properties() {{
      val resource = classOf[KafkaEmbeddedStream].getClassLoader.getResourceAsStream("producer.properties")
      println("==========================================")
      println(resource)
      println("==========================================")
      load(resource)
    }}

    val kafkaProducer = new KafkaProducer[String, String](properties)
    val response = kafkaProducer.send(new ProducerRecord[String, String](stream, event))

    (response.get().offset(), response.get().checksum(), response.get().partition())
  }

  override def consumeEvent(implicit streamConfig: StreamConfig, consumerConfig: ConsumerConfig, stream: String): List[JSONObject] = {
    val nativeKafkaConsumer = new KafkaConsumer[String, String](new Properties() {
      {
        put("bootstrap.servers", s"localhost:${streamConfig.streamTcpPort}")
        put("client.id", s"${consumerConfig.name}")
        put("group.id", s"${consumerConfig.name}_group")
        put("auto.offset.reset", s"${consumerConfig.strategy}")
        put("key.deserializer", classOf[StringDeserializer].getName)
        put("value.deserializer", classOf[StringDeserializer].getName)
      }
    })

    nativeKafkaConsumer.subscribe(Collections.singletonList(stream))

    val events: ConsumerRecords[String, String] = nativeKafkaConsumer.poll(1000)

    events.map(x => new JSONObject(x.value())).toList
  }

  override def assertStreamExists(streamConfig: StreamConfig, stream: String): Unit = {
    assert(AdminUtils.topicExists(new ZkUtils(new ZkClient(s"localhost:${streamConfig.streamStateTcpPort}",
      10000, 15000), new ZkConnection(s"localhost:${streamConfig.streamStateTcpPort}"), false), stream))
  }

  override def createStreamAndWait(stream: String, partition: Int): (String, String) = (null, null)

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

    val zkAddress = s"localhost:${config.streamStateTcpPort}"

    val properties: Properties = new Properties
    properties.setProperty("zookeeper.connect", zkAddress)
    properties.setProperty("broker.id", "0")
    properties.setProperty("host.name", "localhost")
    properties.setProperty("advertised.host.name", "localhost")
    properties.setProperty("auto.create.topics.enable", "true")
    properties.setProperty("port", config.streamTcpPort.toString)
    properties.setProperty("log.dir", kafkaLogDir.toAbsolute.path)
    properties.setProperty("log.flush.interval.messages", 1.toString)

    // The total memory used for log deduplication across all cleaner threads, keep it small to not exhaust suite memory
    properties.setProperty("log.cleaner.dedupe.buffer.size", "1048577")

    config.nodes.foreach {
      case (key, value) => properties.setProperty(key, value)
    }

    val broker = new KafkaServer(new KafkaConfig(properties))
    broker.startup()
    broker
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
