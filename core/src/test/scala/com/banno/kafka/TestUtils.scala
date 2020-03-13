package com.banno.kafka

import kafka.server.{KafkaConfig, KafkaServer}
import org.apache.kafka.common.utils.Time

object TestUtils {

  /**
   * Create a kafka server instance with appropriate test settings
   * USING THIS IS A SIGN YOU ARE NOT WRITING A REAL UNIT TEST
   *
   * @param config The configuration of the server
   */
  def createServer(config: KafkaConfig, time: Time = Time.SYSTEM): KafkaServer = {
    createServer(config, time, None)
  }

  def createServer(config: KafkaConfig, time: Time, threadNamePrefix: Option[String]): KafkaServer = {
    val server = new KafkaServer(config, time, threadNamePrefix = threadNamePrefix)
    server.startup()
    server
  }
}
