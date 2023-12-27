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

import cats.syntax.all.*
import cats.effect.{Sync, IO}
import munit.CatsEffectSuite
import org.scalacheck.Gen
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.producer.ProducerRecord
import com.banno.kafka.admin.AdminApi
import com.banno.kafka.producer.ProducerApi

/*
verify outer effect succeeds as soon as send returns
verify inner effect succeeds as soon as kafka acks (callback is called/record is written)

verify outer effect fails after max.block.ms
verify inner effect fails after delivery.timeout.ms

verify outer effect fails on send throw
verify inner effect fails on callback with exception

verify batching? or sequencing/traversing multiple effects?

verify cancelation?
 */

class ProducerSendSpec extends CatsEffectSuite {

  val bootstrapServer = "localhost:9092"
  val schemaRegistryUrl = "http://localhost:8091"

  def randomId: String =
    Gen.listOfN(10, Gen.alphaChar).map(_.mkString).sample.get
  // def genGroupId: String = randomId
  def genTopic: String = randomId

  def createTestTopic[F[_]: Sync](partitionCount: Int = 1): F[String] = {
    val topicName = genTopic
    AdminApi
      .createTopicsIdempotent[F](
        bootstrapServer,
        List(new NewTopic(topicName, partitionCount, 1.toShort)),
      )
      .as(topicName)
  }

  test("send one record") {
    ProducerApi
      .resource[IO, String, String](
        BootstrapServers(bootstrapServer)
      )
      .use { producer =>
        for {
          topic <- createTestTopic[IO]()
          ack <- producer.send(new ProducerRecord(topic, "a", "a"))
          rm <- ack
        } yield {
          assertEquals(rm.topic, topic)
          assertEquals(rm.offset, 0L)
        }
      }
  }
}
