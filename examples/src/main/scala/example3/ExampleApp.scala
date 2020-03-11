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

package example3

import cats.effect._
import cats.implicits._
import fs2.Stream
import com.banno.kafka._
import com.banno.kafka.admin._
import com.banno.kafka.consumer._
import com.banno.kafka.producer._
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.producer.ProducerRecord
import scala.concurrent.duration._
import scala.util.Random

final class ExampleApp[F[_]: Concurrent: ContextShift: Timer] {

  // Change these for your environment as needed
  val topic = new NewTopic(s"example3", 1, 3.toShort)
  val kafkaBootstrapServers = "kafka.local:9092,kafka.local:9093"

  val example: F[Unit] =
    for {
      _ <- Sync[F].delay(println("Starting kafka4s example"))

      _ <- AdminApi.createTopicsIdempotent[F](kafkaBootstrapServers, topic)

      writeStream = Stream.resource(
        ProducerApi
          .resource[F, Int, Int](BootstrapServers(kafkaBootstrapServers))
          .map(
            p =>
              Timer[F]
                .sleep(1 second)
                .flatMap(
                  _ =>
                    Sync[F]
                      .delay(Random.nextInt())
                      .flatMap(i => p.sendAndForget(new ProducerRecord(topic.name, i, i)))
                )
          )
      )

      readStream = Stream
        .resource(
          ConsumerApi
            .resource[F, Int, Int](
              BootstrapServers(kafkaBootstrapServers),
              GroupId("example3"),
              AutoOffsetReset.earliest,
              EnableAutoCommit(true)
            )
        )
        .evalTap(_.subscribe(topic.name))
        .flatMap(
          _.recordStream(1.second)
            .map(_.value)
            .filter(_ % 2 == 0)
            .evalMap(i => Sync[F].delay(println(i)))
        )

      _ <- writeStream
        .merge(readStream)
        .onFinalize(Sync[F].delay(println("Finished kafka4s example")))
        .compile
        .drain
    } yield ()
}

object ExampleApp {
  def apply[F[_]: Concurrent: ContextShift: Timer] = new ExampleApp[F]
}
