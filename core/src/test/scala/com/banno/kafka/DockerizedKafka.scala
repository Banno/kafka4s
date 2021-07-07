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

import cats.effect._
import cats.syntax.all._
import org.scalacheck.Gen
import com.banno.kafka.admin.AdminApi
import org.apache.kafka.clients.admin.NewTopic

trait DockerizedKafka {
  val bootstrapServer = "localhost:9092"
  val schemaRegistryUrl = "http://localhost:8091"

  def unsafeRandomId: String =
    Gen.listOfN(10, Gen.alphaChar).map(_.mkString).sample.get

  def createTopic[F[_]: Sync](partitionCount: Int = 1): F[String] =
    for {
      topic <- Sync[F].delay(unsafeRandomId)
      _ <- AdminApi.createTopicsIdempotent[F](
        bootstrapServer,
        List(new NewTopic(topic, partitionCount, 1.toShort))
      )
    } yield topic

}
