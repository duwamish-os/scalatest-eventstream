package org.scalatest.eventstream.kinesis

import java.nio.ByteBuffer
import java.util.{Date, Properties, UUID}

import com.amazonaws.ClientConfiguration
import com.amazonaws.auth.{AWSCredentialsProvider, DefaultAWSCredentialsProviderChain}
import com.amazonaws.regions.Regions
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient
import com.amazonaws.services.dynamodbv2.document.DynamoDB
import com.amazonaws.services.kinesis.AmazonKinesisClient
import com.amazonaws.services.kinesis.model._
import org.json.JSONObject
import org.scalatest.eventstream.events.Event
import org.scalatest.eventstream.{Config, ConsumerConfig, EmbeddedStream, StreamConfig}

import scala.collection.JavaConversions._

/**
  * Created by prayagupd
  * on 2/17/17.
  */

class KinesisEmbeddedStream extends EmbeddedStream {

  private val MAX_ITERATIONS = 6
  private val A_SECOND = 1000
  private val EACH_WAIT = 9

  val strategyMap = Map("earliest" -> "TRIM_HORIZON",
    "latest" -> "LATEST",
  "at_event_offset" -> "AT_SEQUENCE_NUMBER")

  val config = Config.getConfig

  private val AppConfig = new Properties(){{
    load(this.getClass.getClassLoader.getResourceAsStream(config))
  }}

  private val ProxyHostOpt = Option[String](AppConfig.getProperty("stream.http.proxy.host"))
  private val PortOpt = Option[String](AppConfig.getProperty("stream.http.proxy.port"))

  val region = AppConfig.getProperty("stream.region")

  private val awsAuthProfile = AppConfig.getProperty("authentication.profile")

  println(awsAuthProfile)

  private def credentials: AWSCredentialsProvider = {
    Option(awsAuthProfile).map(x => System.setProperty("aws.profile", awsAuthProfile))
    new DefaultAWSCredentialsProviderChain()
  }

  private val httpConfiguration: ClientConfiguration = new ClientConfiguration()
    ProxyHostOpt.map(httpConfiguration.setProxyHost(_))
    PortOpt.map(x => httpConfiguration.setProxyPort(x.toInt))

  private val nativeConsumer = new AmazonKinesisClient(credentials, httpConfiguration)

  if(region != null && region != "") {
    nativeConsumer.withRegion(Regions.fromName(region))
  }

  override def startBroker(implicit streamConfig: StreamConfig) : (String, List[String], String) = {
    println(s"Starting a broker at ${new Date()}")
    createStreamAndWait(streamConfig)
  }

  override def createStreamAndWait(implicit config: StreamConfig): (String, List[String], String) = {
    val created = nativeConsumer.createStream(config.stream, config.numOfPartition).getSdkHttpMetadata.getHttpStatusCode == 200
    assert(created)
    waitWhileStreamIsActed(config.stream, "ACTIVE")
    val desc = nativeConsumer.describeStream(config.stream)
    (desc.getStreamDescription.getStreamName, desc.getStreamDescription.getShards.map(_.getShardId).toList,
      desc.getStreamDescription.getStreamStatus)
  }

  override def destroyBroker(implicit streamConfig: StreamConfig): Unit = {
    println(s"Destroying a broker at ${new Date()} with dropping ${streamConfig.stream}")
    try {
      val deleted = nativeConsumer.deleteStream(streamConfig.stream).getSdkHttpMetadata.getHttpStatusCode == 200
      assert(deleted)
      waitWhileStreamIsActed(streamConfig.stream, "DELETED")

      try {
        val desc = nativeConsumer.describeStream(streamConfig.stream)
        assert(desc.getStreamDescription.getStreamStatus == "DELETED")
        (desc.getStreamDescription.getStreamName, desc.getStreamDescription.getStreamStatus)
      } catch {
        //stream could have been deleted successfully, so query would fail
        case e: AmazonKinesisException => println("Drop request submitted, " +
          "but error occured while querying the stream status", e.getMessage)
      }
    } catch {
      case e: AmazonKinesisException => println("Couldn't submit Drop request, " +
        "error occured while querying the stream status", e.getMessage)
    }
  }

  override def appendEvent(stream: String, event: String): Event = {
    val nativeEvent = new PutRecordRequest()
    nativeEvent.setStreamName(stream)
    nativeEvent.setData(ByteBuffer.wrap(event.getBytes))
    nativeEvent.setPartitionKey(UUID.randomUUID().toString) //TODO

    val response = nativeConsumer.putRecord(nativeEvent)

    Event(response.getSequenceNumber, 0l, response.getShardId)
  }

  override def consumeEvent(implicit streamConfig: StreamConfig, consumerConfig: ConsumerConfig, stream: String):
  List[JSONObject] = {
    val getShardIteratorRequest: GetShardIteratorRequest = new GetShardIteratorRequest
    getShardIteratorRequest.setStreamName(stream)
    getShardIteratorRequest.setShardId(consumerConfig.partitionId)
    getShardIteratorRequest.setShardIteratorType(strategyMap(consumerConfig.strategy))

    val iterator = nativeConsumer.getShardIterator(getShardIteratorRequest).getShardIterator

    val recordsRequest = new GetRecordsRequest()
    recordsRequest.setShardIterator(iterator)
    recordsRequest.setLimit(10)

    println(s"consuming partition - ${consumerConfig.partitionId} ${iterator}")

    var events = nativeConsumer.getRecords(recordsRequest)

    if(events.getRecords.isEmpty) {
      Thread.sleep(A_SECOND)
    }

    events = nativeConsumer.getRecords(recordsRequest)

    events.getRecords.map(payloadBytes => new String(payloadBytes.getData.array()))
      .map(json => new JSONObject(json)).toList
  }

  def waitWhileStreamIsActed(stream: String, expectedStatus: String) = {
    var iteration = 0
    while (!streamIsActed(stream, expectedStatus) && iteration < MAX_ITERATIONS) {
      Thread.sleep(EACH_WAIT * A_SECOND)
      println(s"waited ${iteration * EACH_WAIT} secs")
      iteration = iteration + 1
    }
    if (!streamIsActed(stream, expectedStatus)) {
      println(s"Could not process in ${MAX_ITERATIONS * EACH_WAIT} seconds")
      throw new RuntimeException(s"Could not process in ${MAX_ITERATIONS * 10} seconds")
    }
  }

  def streamIsActed(stream: String, expectedStatus: String) = {
    try {
      val actualStatus = nativeConsumer.describeStream(stream).getStreamDescription.getStreamStatus
      println(s"stream $stream is $actualStatus == waiting to be ${expectedStatus}")
      actualStatus.equals(expectedStatus)
    } catch {
      case e : AmazonKinesisException => {
        println(s"Error querying the stream ${stream}, as it might have been " +
          s"already deleted, ${e.getMessage}.")
        true
      }
    }
  }

  /**
    * http://docs.aws.amazon.com/kinesis/latest/APIReference/API_GetShardIterator.html
    */
  def findEventByEventId(implicit streamConfig: StreamConfig, consumerConfig: ConsumerConfig, stream: String,
                         eventId: String): List[JSONObject] = {
    val getShardIteratorRequest: GetShardIteratorRequest = new GetShardIteratorRequest
    getShardIteratorRequest.setStreamName(stream)
    getShardIteratorRequest.setShardId(consumerConfig.partitionId)
    getShardIteratorRequest.setShardIteratorType(strategyMap(consumerConfig.strategy))
    getShardIteratorRequest.setStartingSequenceNumber(eventId)

    val iterator = nativeConsumer.getShardIterator(getShardIteratorRequest).getShardIterator

    val recordsRequest = new GetRecordsRequest()
    recordsRequest.setShardIterator(iterator)
    recordsRequest.setLimit(1)

    println(s"consuming bytes - ${consumerConfig.partitionId} ${iterator}")

    var events = nativeConsumer.getRecords(recordsRequest)

    if(events.getRecords.isEmpty) {
      Thread.sleep(A_SECOND)
    }

    events = nativeConsumer.getRecords(recordsRequest)

    events.getRecords.map(payloadBytes => new String(payloadBytes.getData.array()))
      .map(json => new JSONObject(json)).toList
  }

  override def describeStream(implicit streamConfig: StreamConfig): Boolean = false //TODO

  override def assertStreamExists(streamConfig: StreamConfig): Unit =  {
    val actualStatus = nativeConsumer.describeStream(streamConfig.stream).getStreamDescription.getStreamStatus
    assert(actualStatus == "ACTIVE")
  }

  override def listStreams(implicit streamConfig: StreamConfig): List[String] = {
    List()
  }

  override def dropConsumerState(stateTable: String): String = {
    val consumerOffset = new AmazonDynamoDBClient(credentials, httpConfiguration)
    if(region != null && region != "") {
      consumerOffset.withRegion(Regions.fromName(region))
    }

    val dynamoDB = new DynamoDB(consumerOffset)
    val deleteState = dynamoDB.getTable(stateTable).delete()
    assert(deleteState.getSdkHttpMetadata.getHttpStatusCode == 200)
    Thread.sleep(A_SECOND)
    deleteState.getTableDescription.getTableStatus
  }
}
