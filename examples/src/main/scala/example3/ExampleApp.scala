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
  val topic = new NewTopic(s"example3", 1, 3)
  val kafkaBootstrapServers = "kafka.local:9092,kafka.local:9093"

  val producer = Stream.resource(ProducerApi.resource[F, Int, Int](BootstrapServers(kafkaBootstrapServers)))

  val consumer = Stream.resource(ConsumerApi.resource[F, Int, Int](
    BootstrapServers(kafkaBootstrapServers),
    GroupId("example3"),
    AutoOffsetReset.earliest,
    EnableAutoCommit(true)
  ))

  def randomInt: F[Int] = Sync[F].delay(Random.nextInt())

  val example: F[Unit] =
    for {
      _ <- Sync[F].delay(println("Starting kafka4s example"))

      _ <- AdminApi.createTopicsIdempotent[F](kafkaBootstrapServers, topic)

      writeStream = producer.flatMap(p =>
        Stream.awakeDelay[F](1 second).evalMap(_ => randomInt.flatMap(i => p.sendAndForget(new ProducerRecord(topic.name, i, i))))
      )

      readStream = consumer.flatMap(c =>
        c.recordStream(
          initialize = c.subscribe(topic.name),
          pollTimeout = 1.second
        )
        .map(_.value)
        .filter(_ % 2 == 0)
        .evalMap(i => Sync[F].delay(println(i)))
      )

      _ <- writeStream.merge(readStream).onFinalize(Sync[F].delay(println("Finished kafka4s example"))).compile.drain
    } yield ()
}

object ExampleApp {
  def apply[F[_]: Concurrent: ContextShift: Timer] = new ExampleApp[F]
}
