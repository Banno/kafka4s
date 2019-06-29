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
  val topicName = s"topic-${java.util.UUID.randomUUID()}"
  val topic = new NewTopic(topicName, 1, 3)
  val kafkaBootstrapServers = "kafka.local:9092,kafka.local:9093"
  val schemaRegistryUri = "http://kafka.local:8081"

  val producerRecords: Vector[ProducerRecord[CustomerId, CustomerRecord]] = (1 to 10)
    .map(
      a =>
        new ProducerRecord(
          topic.name,
          CustomerId(a.toString),
          CustomerRecord(Customer(s"name-${a}", s"address-${a}"))
        )
    )
    .toVector

  val example: F[Unit] =
    for {
      _ <- Sync[F].delay(println("Starting kafka4s example"))

      _ <- AdminApi.createTopicsIdempotent[F](kafkaBootstrapServers, topic)
      _ <- Sync[F].delay(println(s"Created topic ${topic.name}"))

      schemaRegistry <- SchemaRegistryApi(schemaRegistryUri)
      _ <- schemaRegistry.registerKey[CustomerId](topic.name)
      _ <- Sync[F].delay(println(s"Registered key schema for topic ${topic.name}"))
      _ <- schemaRegistry.registerValue[CustomerRecord](topic.name)
      _ <- Sync[F].delay(println(s"Registered value schema for topic ${topic.name}"))

      producer <- ProducerApi.avro4sShifting[F, CustomerId, CustomerRecord](
        ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(1)),
        BootstrapServers(kafkaBootstrapServers),
        SchemaRegistryUrl(schemaRegistryUri),
        ClientId("producer-example")
      )

      _ <- producerRecords.traverse_(
        pr =>
          producer.sendSync(pr) *> Sync[F]
            .delay(println(s"Wrote producer record: key ${pr.key} and value ${pr.value}"))
      )

      consumer <- ConsumerApi.avro4sShifting[F, CustomerId, CustomerRecord](
        BootstrapServers(kafkaBootstrapServers),
        SchemaRegistryUrl(schemaRegistryUri),
        ClientId("consumer-example"),
        GroupId("consumer-example-group"),
        EnableAutoCommit(false)
      )

      _ <- consumer
        .recordStream(
          initialize = consumer.assign(topic.name, Map.empty[TopicPartition, Long]),
          pollTimeout = 1.second
        )
        .take(producerRecords.size.toLong)
        .evalMap(
          cr => Sync[F].delay(println(s"Read consumer record: key ${cr.key} and value ${cr.value}"))
        )
        .compile
        .drain

      _ <- Sync[F].delay(println("Finished kafka4s example"))
    } yield ()
}

object ExampleApp {
  def apply[F[_]: Async: ContextShift] = new ExampleApp[F]
}
