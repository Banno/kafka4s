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

package example2

import cats.effect._
import cats.syntax.all._
import com.banno.kafka._
import com.banno.kafka.admin._
import com.banno.kafka.schemaregistry._
import com.banno.kafka.consumer._
import com.banno.kafka.producer._
import com.banno.kafka.avro4s._
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.producer.ProducerRecord
import scala.concurrent.duration._
import org.apache.kafka.common.TopicPartition

final class ExampleApp[F[_]: Async] {

  // Change these for your environment as needed
  val topic = new NewTopic(s"example1.customers.v1", 1, 3.toShort)
  val kafkaBootstrapServers = "kafka.local:9092,kafka.local:9093"
  val schemaRegistryUri = "http://kafka.local:8081"

  val producerRecords: Vector[ProducerRecord[CustomerId, Customer]] = (11 to 20)
    .map(
      a =>
        new ProducerRecord(
          topic.name,
          CustomerId(a.toString),
          Customer(
            name = s"name-${a}",
            address = s"address-${a}",
            priority = a % 3 match {
              case 0 => None
              case 1 => Some(Priority.Gold)
              case 2 => Some(Priority.Platinum)
            }
          )
        )
    )
    .toVector

  val producer =
    Avro4sProducer.resource[F, CustomerId, Customer](
      BootstrapServers(kafkaBootstrapServers),
      SchemaRegistryUrl(schemaRegistryUri),
      ClientId("producer-example")
    )

  val consumer =
    Avro4sConsumer.resource[F, CustomerId, Customer](
      BootstrapServers(kafkaBootstrapServers),
      SchemaRegistryUrl(schemaRegistryUri),
      ClientId("consumer-example"),
      GroupId("consumer-example-group"),
      EnableAutoCommit(false)
    )

  val example: F[Unit] =
    for {
      _ <- Sync[F].delay(println("Starting kafka4s example"))

      implicit0(factory: Avro4sSchematic.Factory) <- Avro4sSchematic.default[F]
      _ <- AdminApi.createTopicsIdempotent[F](kafkaBootstrapServers, topic)
      _ <- SchemaRegistryApi.register[F, CustomerId, Customer](schemaRegistryUri, topic.name)

      _ <- producer
        .use(
          producer =>
            producerRecords.traverse_(
              pr =>
                producer.sendSync(pr) *> Sync[F]
                  .delay(println(s"Wrote producer record: key ${pr.key} and value ${pr.value}"))
            )
        )

      _ <- consumer.use(
        consumer =>
          consumer.assign(topic.name, Map.empty[TopicPartition, Long]) *>
            consumer
              .recordStream(1.second)
              .take(20L)
              .evalMap(
                cr =>
                  Sync[F]
                    .delay(println(s"Read consumer record: key ${cr.key} and value ${cr.value}"))
              )
              .compile
              .drain
      )

      _ <- Sync[F].delay(println("Finished kafka4s example"))
    } yield ()
}

object ExampleApp {
  def apply[F[_]: Async] = new ExampleApp[F]
}
