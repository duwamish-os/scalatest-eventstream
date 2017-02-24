package org.scalatest.eventstream

import org.json.JSONObject

/**
  * interface for eventstream
  *
  * Created by prayagupd
  * on 2/10/17.
  */

trait EmbeddedStream {
  def startBroker(implicit eventStreamConfig: StreamConfig): (String, List[String], String)
  def destroyBroker(implicit eventStreamConfig: StreamConfig)

  def createStreamAndWait(stream: String, partition: Int) : (String, List[String], String)
  def appendEvent(stream:String, event: String) : (String, Long, String)
  def consumeEvent(implicit streamConfig: StreamConfig, consumerConfig: ConsumerConfig, stream: String): List[JSONObject]
  def assertStreamExists(streamConfig: StreamConfig): Unit
  def dropConsumerState(stateTable : String) : String
}
