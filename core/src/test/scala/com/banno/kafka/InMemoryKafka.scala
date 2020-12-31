package com.banno.kafka

import org.scalatest.{BeforeAndAfterAll, Suite}
import org.scalacheck.Gen
import com.banno.kafka.admin.AdminApi
import cats.effect.IO
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.apache.kafka.clients.admin.NewTopic

trait InMemoryKafka extends BeforeAndAfterAll { this: Suite =>

  val log = Slf4jLogger.getLoggerFromClass[IO](this.getClass)

  val bootstrapServer = "localhost:9092"
  val schemaRegistryUrl = "http://localhost:8091"

  override def beforeAll(): Unit =
    log.info(s"Using docker-machine Kafka cluster for ${getClass.getName}").unsafeRunSync()

  override def afterAll(): Unit =
    log.info(s"Used docker-machine Kafka cluster for ${getClass.getName}").unsafeRunSync()

  def randomId: String = Gen.listOfN(10, Gen.alphaChar).map(_.mkString).sample.get
  def genGroupId: String = randomId
  def genTopic: String = randomId
  def createTopic(partitionCount: Int = 1): String = {
    val topic = genTopic
    AdminApi
      .createTopicsIdempotent[IO](
        bootstrapServer,
        List(new NewTopic(topic, partitionCount, 1.toShort))
      )
      .unsafeRunSync()
    topic
  }

}
