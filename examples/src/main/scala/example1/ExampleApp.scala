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

package example1

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
import java.util.concurrent.Executors
import org.apache.kafka.common.TopicPartition
import scala.concurrent.ExecutionContext

final class ExampleApp[F[_]: Async: ContextShift] {
  import ExampleApp._

  // Change these for your environment as needed
  val topic = new NewTopic(s"example1.customers.v1", 1, 3)
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

  val producerThreadPoolResource = Resource.make(
    Sync[F].delay(ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(1)))
  )(a => Sync[F].delay(a.shutdown()))

  val producerResource: Resource[F, ProducerApi[F, CustomerId, Customer]] =
    producerThreadPoolResource.flatMap(
      producerPool =>
        Resource.make(
          ProducerApi.avro4sShifting[F, CustomerId, Customer](
            producerPool,
            BootstrapServers(kafkaBootstrapServers),
            SchemaRegistryUrl(schemaRegistryUri),
            ClientId("producer-example")
          )
        )(_.close)
    )

  val consumerResource =
    Resource.make(
      ConsumerApi.Avro4s.create[F, CustomerId, Customer](
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
      _ <- Sync[F].delay(println(s"Created topic ${topic.name}"))

      schemaRegistry <- SchemaRegistryApi(schemaRegistryUri)
      _ <- schemaRegistry.registerKey[CustomerId](topic.name)
      _ <- Sync[F].delay(println(s"Registered key schema for topic ${topic.name}"))

      _ <- schemaRegistry.registerValue[Customer](topic.name)
      _ <- Sync[F].delay(println(s"Registered value schema for topic ${topic.name}"))

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
          consumer.assign(topic.name, Map.empty[TopicPartition, Long]) *>
            consumer
              .recordStream(1.second)
              .take(producerRecords.size.toLong)
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
  case class CustomerId(id: String)
  case class Customer(name: String, address: String)

  def apply[F[_]: Async: ContextShift] = new ExampleApp[F]
}
