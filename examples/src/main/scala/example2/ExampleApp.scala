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
import cats.implicits._
import com.banno.kafka._
import com.banno.kafka.admin._
import com.banno.kafka.schemaregistry._
import com.banno.kafka.consumer._
import com.banno.kafka.producer._
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.producer.ProducerRecord
import scala.concurrent.duration._
import org.apache.kafka.common.TopicPartition

final class ExampleApp[F[_]: Async: ContextShift] {
  import ExampleApp._

  // Change these for your environment as needed
  val topic = new NewTopic(s"example2.customers", 1, 3)
  val kafkaBootstrapServers = "kafka.local:9092,kafka.local:9093"
  val schemaRegistryUri = "http://kafka.local:8081"

  val producerRecords: Vector[ProducerRecord[CustomerId, Customer]] = (1 to 10)
    .map(
      a =>
        new ProducerRecord(
          topic.name,
          CustomerId(a.toString),
          Customer(s"name-${a}", s"address-${a}")
        )
    )
    .toVector

  val producerResource: Resource[F, ProducerApi[F, CustomerId, Customer]] =
    ProducerApi.defaultBlockingContext.flatMap(
      ProducerApi.resourceAvro4sShifting(
        _,
        BootstrapServers(kafkaBootstrapServers),
        SchemaRegistryUrl(schemaRegistryUri),
        ClientId("producer-example")
      )
    )

  val consumerResource = Resource.make(
    ConsumerApi.avro4sShifting[F, CustomerId, Customer](
      BootstrapServers(kafkaBootstrapServers),
      SchemaRegistryUrl(schemaRegistryUri),
      ClientId("consumer-example"),
      GroupId("consumer-example-group"),
      EnableAutoCommit(false)
    )
  )(_.close)

  val example: F[Unit] =
    for {
      _ <- Sync[F].delay(println("Starting kafka4s example"))

      _ <- AdminApi.createTopicsIdempotent[F](kafkaBootstrapServers, topic)
      _ <- SchemaRegistryApi.register[F, CustomerId, Customer](schemaRegistryUri, topic.name)

      _ <- producerResource.use(
        producer =>
          producerRecords.traverse_(
            pr =>
              producer.sendSync(pr) *> Sync[F]
                .delay(println(s"Wrote producer record: key ${pr.key} and value ${pr.value}"))
          )
      )

      _ <- consumerResource.use(
        consumer =>
          consumer
            .recordStream(
              initialize = consumer.assign(topic.name, Map.empty[TopicPartition, Long]),
              pollTimeout = 1.second
            )
            .take(producerRecords.size.toLong)
            .evalMap(
              cr =>
                Sync[F].delay(println(s"Read consumer record: key ${cr.key} and value ${cr.value}"))
            )
            .compile
            .drain
      )

      _ <- Sync[F].delay(println("Finished kafka4s example"))
    } yield ()
}

object ExampleApp {
  case class CustomerId(id: String)
  case class Customer(name: String, address: String)

  def apply[F[_]: Async: ContextShift] = new ExampleApp[F]
}
