package org.scalatest.eventstream.kinesis

import java.util.Date

import com.amazonaws.ClientConfiguration
import com.amazonaws.auth.profile.ProfileCredentialsProvider
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient
import com.amazonaws.services.dynamodbv2.document.DynamoDB
import com.amazonaws.services.kinesis.AmazonKinesisClient
import com.typesafe.config.ConfigFactory
import org.json.JSONObject
import org.scalatest.eventstream.{ConsumerConfig, EmbeddedStream, StreamConfig}

/**
  * Created by prayagupd
  * on 2/17/17.
  */

class KinesisEmbeddedStream extends EmbeddedStream {

  private val MAX_ITERATIONS = 6
  private val PROXYHOST: String = ConfigFactory.load("application.properties").getString("stream.http.proxy.host")
  private val PORT: Int = ConfigFactory.load("application.properties").getInt("stream.http.proxy.port")

  private val awsAuthProfile = ConfigFactory.load("application.properties").getString("authentication.profile")

  private val credentials: ProfileCredentialsProvider = new ProfileCredentialsProvider(awsAuthProfile)
  private val httpConfiguration: ClientConfiguration = new ClientConfiguration()
    .withProxyHost(PROXYHOST).withProxyPort(PORT)

  private val nativeConsumer = new AmazonKinesisClient(credentials, httpConfiguration)

  override def startBroker(implicit streamConfig: StreamConfig): Unit = {
    println(s"Starting a broker at ${new Date()}")
    createStreamAndWait(streamConfig.stream, streamConfig.partition)
  }

  override def createStreamAndWait(stream: String, partition: Int): (String, String) = {
    val created = nativeConsumer.createStream(stream, partition).getSdkHttpMetadata.getHttpStatusCode == 200
    assert(created)
    waitWhileStreamIsActed(stream, "ACTIVE")
    val desc = nativeConsumer.describeStream(stream)
    (desc.getStreamDescription.getStreamName, desc.getStreamDescription.getStreamStatus)
  }

  override def destroyBroker(implicit streamConfig: StreamConfig): Unit = {
    println(s"Destroying a broker at ${new Date()} with dropping ${streamConfig.stream}")
    val deleted = nativeConsumer.deleteStream(streamConfig.stream).getSdkHttpMetadata.getHttpStatusCode == 200
    assert(deleted)

    waitWhileStreamIsActed(streamConfig.stream, "DELETED")
    val desc = nativeConsumer.describeStream(streamConfig.stream)
    assert(desc.getStreamDescription.getStreamStatus == "DELETED")
    (desc.getStreamDescription.getStreamName, desc.getStreamDescription.getStreamStatus)
  }

  override def appendEvent(stream: String, event: String): (Long, Long, Int) = null

  override def consumeEvent(implicit streamConfig: StreamConfig, consumerConfig: ConsumerConfig, stream: String): List[JSONObject] = List.empty

  def waitWhileStreamIsActed(stream: String, expectedStatus: String) = {
    var iteration = 0
    while(!streamIsActed(stream, expectedStatus) && iteration < MAX_ITERATIONS){
      Thread.sleep(10 * 1000)
      iteration = iteration + 1
    }
    if(!streamIsActed(stream, expectedStatus)) {
      println(s"Could not process in ${MAX_ITERATIONS * 10} seconds")
      throw new RuntimeException(s"Could not process in ${MAX_ITERATIONS * 10} seconds")
    }
  }

  def streamIsActed(stream: String, expectedStatus: String) = {
    val actualStatus = nativeConsumer.describeStream(stream).getStreamDescription.getStreamStatus
    println(s"stream $stream is $actualStatus== waiting to be ${expectedStatus}")
    actualStatus.equals(expectedStatus)
  }

  override def assertStreamExists(streamConfig: StreamConfig, stream: String): Unit = assert(1 == 2)

  override def dropConsumerState(stateTable: String): String = {
    val consumerOffset = new AmazonDynamoDBClient(credentials, httpConfiguration)
    val dynamoDB = new DynamoDB(consumerOffset)
    val deleteState = dynamoDB.getTable(stateTable).delete()
    assert(deleteState.getSdkHttpMetadata.getHttpStatusCode == 200)
    Thread.sleep(1000)
    deleteState.getTableDescription.getTableStatus
  }
}
