package com.banno.kafka

import org.scalatest.{BeforeAndAfterAll, Suite}
import io.confluent.examples.streams.kafka.EmbeddedSingleNodeKafkaCluster
import org.scalacheck.Gen
import com.banno.kafka.admin.AdminApi
import cats.effect.IO
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.apache.kafka.clients.admin.NewTopic

trait InMemoryKafka extends BeforeAndAfterAll { this: Suite =>

  val log = Slf4jLogger.getLoggerFromClass[IO](this.getClass)

  val kafkaCluster = new EmbeddedSingleNodeKafkaCluster()
  def bootstrapServer = kafkaCluster.bootstrapServers()
  def schemaRegistryUrl = kafkaCluster.schemaRegistryUrl()

  override def beforeAll(): Unit = {
    log.info(s"Starting in-memory Kafka cluster for ${getClass.getName}").unsafeRunSync()
    kafkaCluster.start()
  }

  override def afterAll(): Unit = {
    log.info(s"Stopping in-memory Kafka cluster for ${getClass.getName}").unsafeRunSync()
    kafkaCluster.stop()
  }

  def randomId: String = Gen.listOfN(10, Gen.alphaChar).map(_.mkString).sample.get
  def genGroupId: String = randomId
  def genTopic: String = randomId
  def createTopic(partitionCount: Int = 1): String = {
    val topic = genTopic
    AdminApi
      .createTopicsIdempotent[IO](bootstrapServer, List(new NewTopic(topic, partitionCount, 1)))
      .unsafeRunSync()
    topic
  }

}
