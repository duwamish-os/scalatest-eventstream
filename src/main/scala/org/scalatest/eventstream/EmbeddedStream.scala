package org.scalatest.eventstream

import org.json.JSONObject

/**
  * interface for eventstream
  *
  * Created by prayagupd
  * on 2/10/17.
  */

trait EmbeddedStream {
  def startBroker(implicit eventStreamConfig: StreamConfig)
  def destroyBroker(implicit eventStreamConfig: StreamConfig)

  def createStreamAndWait(stream: String, partition: Int) : (String, String)
  def appendEvent(stream:String, event: String) : (Long, Long, Int)
  def consumeEvent(implicit streamConfig: StreamConfig, consumerConfig: ConsumerConfig, stream: String): List[JSONObject]
  def assertStreamExists(streamConfig: StreamConfig, stream: String): Unit
  def dropConsumerState(stateTable : String) : String
}
