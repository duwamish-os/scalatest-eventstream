package org.scalatest.eventstream

import org.json.JSONObject
import org.scalatest.eventstream.events.Event

/**
  * interface for eventstream
  *
  * Created by prayagupd
  * on 2/10/17.
  */

trait EmbeddedStream {
  def startBroker(implicit eventStreamConfig: StreamConfig): (String, List[String], String)
  def destroyBroker(implicit eventStreamConfig: StreamConfig)

  def createStreamAndWait(implicit eventStreamConfig: StreamConfig) : (String, List[String], String)
  def assertStreamExists(streamConfig: StreamConfig): Unit
  def listStreams(implicit streamConfig: StreamConfig): List[String]
  def describeStream(implicit streamConfig: StreamConfig) : Boolean //FIXME return type to case class Stream(name, numberOfPartitions, retentionPeriod)
  def appendEvent(stream:String, event: String) : Event
  def consumeEvent(implicit streamConfig: StreamConfig, consumerConfig: ConsumerConfig, stream: String): List[JSONObject]
  def dropConsumerState(stateTable : String) : String
}
