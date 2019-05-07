package kafka.utils

import org.I0Itec.zkclient.{ZkClient, ZkConnection}

/**
  * Created by prayagupd
  * on 6/3/17.
  */

object StateServer {

  def createConnection(stateEndpoint: String): (ZkClient, ZkUtils) = {

    val zookeeperHosts = stateEndpoint
    val sessionTimeOutInMs = 15 * 1000
    val connectionTimeOutInMs = 10 * 1000

    val zkClient = new ZkClient(
      zookeeperHosts,
      sessionTimeOutInMs,
      connectionTimeOutInMs,
      ZKStringSerializer
    )
    val zkUtils = new ZkUtils(zkClient, new ZkConnection(zookeeperHosts), false)

    (zkClient, zkUtils)
  }

}
