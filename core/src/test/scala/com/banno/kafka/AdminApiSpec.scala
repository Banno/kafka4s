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
import cats.implicits._
import org.apache.kafka.clients.admin._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

class AdminApiSpec extends AnyFlatSpec with Matchers with InMemoryKafka {
  implicit val defaultTimer = IO.timer(ExecutionContext.global)

  //Probably don't need to test every single AdminClient operation; this is just a sanity check that it is all wired up properly

  "Admin API" should "create topics idempotently" in {
    val topicName = genTopic
    def program[F[_]: Timer](admin: AdminApi[F])(implicit F: Sync[F]) =
      for {
        ltr1 <- admin.listTopics
        ns1 <- F.delay(ltr1.names().get())
        _ <- admin.createTopicsIdempotent(List(new NewTopic(topicName, 1, 1.toShort)))
        _ <- admin.createTopicsIdempotent(List(new NewTopic(topicName, 1, 1.toShort)))
        _ <- Timer[F].sleep(1.second) // TODO: Better fix
        ltr2 <- admin.listTopics
        ns2 <- F.delay(ltr2.names.get())
      } yield (ns1, ns2)

    val (before, after) =
      AdminApi.resource[IO](BootstrapServers(bootstrapServer)).use(program[IO]).unsafeRunSync()
    before should not contain (topicName)
    after should contain(topicName)
  }

}
