package org.scalatest.eventstream

/**
  * Created by prayagupd
  * on 4/24/17.
  */

object Config {

  def configExists: Boolean = {
    try {
      Config.getClass.getClassLoader.getResourceAsStream("application-" + env + ".properties")
      true
    } catch {
      case x: Exception => false
    }
  }

  def getConfig: String = {
    if(env != null && configExists) {
      "application-" + env + ".properties"
    } else {
      "application.properties"
    }
  }

  def env: String = {
    System.getenv("APPLICATION_ENVIRONMENT")
  }
}
