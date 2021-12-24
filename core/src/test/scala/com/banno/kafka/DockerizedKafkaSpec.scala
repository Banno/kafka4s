/*
 * Copyright 2019 Jack Henry & Associates, Inc.®
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.banno.kafka

import org.scalatest.{BeforeAndAfterAll, Suite}
import org.scalacheck.Gen
import com.banno.kafka.admin.AdminApi
import cats.effect.IO
import org.typelevel.log4cats.slf4j.Slf4jLogger
import org.apache.kafka.clients.admin.NewTopic

trait DockerizedKafkaSpec extends BeforeAndAfterAll { this: Suite =>
  import cats.effect.unsafe.implicits.global

  val log = Slf4jLogger.getLoggerFromClass[IO](this.getClass)
  val bootstrapServer = "localhost:9092"
  val schemaRegistryUrl = "http://localhost:8091"

  override def beforeAll(): Unit =
    log
      .info(s"Using docker-machine Kafka cluster for ${getClass.getName}")
      .unsafeRunSync()

  override def afterAll(): Unit =
    log
      .info(s"Used docker-machine Kafka cluster for ${getClass.getName}")
      .unsafeRunSync()

  def randomId: String =
    Gen.listOfN(10, Gen.alphaChar).map(_.mkString).sample.get
  def genGroupId: String = randomId
  def genTopic: String = randomId
  def createTopic(partitionCount: Int = 1): String = {
    val topic = genTopic
    AdminApi
      .createTopicsIdempotent[IO](
        bootstrapServer,
        List(new NewTopic(topic, partitionCount, 1.toShort)),
      )
      .unsafeRunSync()
    topic
  }

}
