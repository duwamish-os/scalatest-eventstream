package org.scalatest.eventstream

/**
  * Created by prayagupd
  * on 2/22/17.
  */

case class StreamConfig(streamTcpPort: Int = 9092,
                        streamStateTcpPort :Int = 2181,
                        stream: String,
                        partition: Int = 1,
                        nodes: Map[String, String] = Map.empty)