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

import com.banno.kafka.admin._
import cats.effect._
import cats.syntax.all._
import org.apache.kafka.clients.admin._
import munit._
import scala.concurrent.duration._

class AdminApiSpec extends CatsEffectSuite with DockerizedKafka {
  // Probably don't need to test every single AdminClient operation; this is
  // just a sanity check that it is all wired up properly
  test("Admin API should create topics idempotently") {
    def program[F[_]: Async](admin: AdminApi[F]) =
      for {
        topicName <- Sync[F].delay(unsafeRandomId)
        ltr1 <- admin.listTopics
        ns1 <- Sync[F].delay(ltr1.names().get())
        _ <- admin.createTopicsIdempotent(List(new NewTopic(topicName, 1, 1.toShort)))
        _ <- admin.createTopicsIdempotent(List(new NewTopic(topicName, 1, 1.toShort)))
        _ <- Temporal[F].sleep(1.second) // TODO: Better fix
        ltr2 <- admin.listTopics
        ns2 <- Sync[F].delay(ltr2.names.get())
      } yield (topicName, ns1, ns2)

    AdminApi
      .resource[IO](BootstrapServers(bootstrapServer))
      .use(program[IO])
      .map { tuple =>
        val (topicName, before, after) = tuple
        !before.contains(topicName) && after.contains(topicName)
      }
      .assert
  }

}
