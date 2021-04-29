package com.banno.kafka

import cats.syntax.all._
import cats.effect._
import org.apache.kafka.clients.producer.ProducerRecord

import scala.concurrent.duration._
import com.banno.kafka.producer._
import com.banno.kafka.consumer._
import fs2._

import scala.util.Random
import org.scalacheck.magnolia._
import com.sksamuel.avro4s.RecordFormat
import org.apache.kafka.common.TopicPartition

import scala.jdk.CollectionConverters._
import java.util.ConcurrentModificationException

import org.apache.kafka.common.errors.WakeupException
import munit._
import org.scalacheck._
import org.scalacheck.Prop._
import org.scalacheck.effect.PropF._

class ConsumerAndProducerApiSpec
    extends CatsEffectSuite
    with ScalaCheckEffectSuite
    with DockerizedKafka {
  implicit def noShrink[T]: Shrink[T] = Shrink.shrinkAny

  // Probably don't need to test every single operation; this is just a sanity
  // check that it is all wired up properly

  def producerRecord[K, V](topic: String)(p: (K, V)): ProducerRecord[K, V] =
    new ProducerRecord[K, V](topic, p._1, p._2)

  def writeAndRead[F[_], K, V](
      producer: ProducerApi[F, K, V],
      consumer: ConsumerApi[F, K, V],
      topic: String,
      values: Vector[(K, V)]
  ): Stream[F, (K, V)] =
    Stream
      .emits(values)
      .covary[F]
      .map(producerRecord(topic))
      .evalMap(producer.sendAndForget)
      .drain ++
      Stream.eval(consumer.subscribe(topic)).drain ++
      consumer
        .recordStream(100 millis)
        .map(cr => (cr.key, cr.value))
        .take(values.size.toLong)

  private def assertThrowable(
    e: Either[Throwable, _],
    f: PartialFunction[Throwable, Unit]
  ): Unit =
    assert(e.left.toOption.flatMap(f.lift).nonEmpty)

  test("Producer API sends records and produces metadata") {
    val topic = createTopic[IO]().unsafeRunSync()

    forAllF { strings: Vector[String] =>
      // TODO what does sendAsyncBatch actually do?
      //  1. send one record and semantically block until metadata received,
      //     send next record and semantically block until metadata received,
      //     etc
      //  2. or, send all records in batch, then semantically block until all
      //     metadatas received
      // I suspect it's #1, which may be useful, but most of the time #2 is
      // really what we want, for max throughput.

      ProducerApi
        .resource[IO, String, String](BootstrapServers(bootstrapServer))
        .use(_.sendAsyncBatch(strings.map(s => new ProducerRecord(topic, s, s))))
        .map { rms =>
          assertEquals(rms.size, strings.size)
          assertEquals(rms.forall(_.topic == topic), true)
          if (strings.size >= 2) {
            assertEquals(rms.last.offset - rms.head.offset, (strings.size - 1).toLong)
          }
        }
    }
  }

  // `KafkaConsumer` is not thread-safe; if one thread is calling poll while
  // another concurrently calls close, close will throw
  // `ConcurrentModificationException`
  test("Simple consumer close fails with ConcurrentModificationException while polling".flaky) {
    val topic = createTopic[IO]().unsafeRunSync()
    ConsumerApi.NonShifting
      .resource[IO, String, String](BootstrapServers(bootstrapServer))
      .use(
        c =>
          for {
            _ <- c.assign(topic, Map.empty[TopicPartition, Long])
            f <- Concurrent[IO].start(c.poll(1 second))
            e <- Temporal[IO].sleep(100 millis) *> c.close.attempt
            _ <- f.join
          } yield assertThrowable(e, { case _: ConcurrentModificationException => () })
      )
  }

  // Calling KafkaConsumer.wakeup will cause any other concurrent operation to
  // throw WakeupException
  test("Simple consumer poll fails with WakeupException on wakeup".flaky) {
    val topic = createTopic[IO]().unsafeRunSync()
    ConsumerApi.NonShifting
      .resource[IO, String, String](BootstrapServers(bootstrapServer))
      .use(
        c =>
          for {
            _ <- c.assign(topic, Map.empty[TopicPartition, Long])
            _ <- Concurrent[IO].start(Temporal[IO].sleep(100 millis) *> c.wakeup)
            e <- c.poll(1 second).attempt
          } yield assertThrowable(e, { case _: WakeupException => () })
      )
  }

  // If we recover from close failing with CME by calling wakeup, and recover
  // from poll failing with WE by calling close, we can call poll and close
  // concurrently
  test("Simple consumer close while polling") {
    val topic = createTopic[IO]().unsafeRunSync()
    ConsumerApi.NonShifting
      .resource[IO, String, String](BootstrapServers(bootstrapServer))
      .allocated
      .map {
        case (c, _) =>
          for {
            _ <- c.assign(topic, Map.empty[TopicPartition, Long])
            f <- Concurrent[IO].start(c.pollAndRecoverWakeupWithClose(1 second))
            () <- Temporal[IO].sleep(100 millis)
            e1 <- c.closeAndRecoverConcurrentModificationWithWakeup.attempt
            outcome <- f.join
            e2 <- outcome match {
              case Outcome.Succeeded(fa) => fa
              case _ => IO.raiseError(new RuntimeException("Failed!"))
            }
          } yield {
            assertEquals(e1.toOption.get, ())
            assertEquals(e2.count, 0)
          }
      }
  }

  // If we shift all operations onto a singleton thread pool then they become
  // sequential, so we can safely call poll and close concurrently without
  // requiring recovery & wakeup
  test("Singleton shifting consumer close while polling") {
    val topic = createTopic[IO]().unsafeRunSync()
    ConsumerApi
      .resource[IO, String, String](BootstrapServers(bootstrapServer))
      .allocated
      .map {
        case (c, close) =>
          for {
            _ <- c.assign(topic, Map.empty[TopicPartition, Long])
            _ <- Concurrent[IO].start(c.poll(1 second))
            e <- Temporal[IO].sleep(100 millis) *> close.attempt
          } yield {
            assertEquals(e.toOption.get, ())
          }
      }
  }

  // wakeup is the one thread-safe operation, so we don't need to shift it
  test("Singleton shifting consumer poll fails with WakeupException on wakeup") {
    val topic = createTopic[IO]().unsafeRunSync()
    ConsumerApi
      .resource[IO, String, String](BootstrapServers(bootstrapServer))
      .use(
        c =>
          for {
            _ <- c.assign(topic, Map.empty[TopicPartition, Long])
            _ <- Concurrent[IO].start(Temporal[IO].sleep(100 millis) *> c.wakeup)
            e <- c.poll(1 second).attempt
          } yield assertThrowable(e, { case _: WakeupException => () })
      )
  }

  test("Producer and Consumer APIs should write and read records") {
    val groupId = unsafeRandomId
    val topic = createTopic[IO]().unsafeRunSync()

    forAllF { values: Vector[(String, String)] =>
      val fa = for {
        p <- Stream.resource(
          ProducerApi.resource[IO, String, String](BootstrapServers(bootstrapServer))
        )
        c <- Stream.resource(
          ConsumerApi.resource[IO, String, String](
            BootstrapServers(bootstrapServer),
            GroupId(groupId),
            AutoOffsetReset.earliest
          )
        )
        x <- writeAndRead(p, c, topic, values)
      } yield x
      fa.compile.toVector.map(assertEquals(_, values))
    }
  }

  test("read through final offsets") {
    val topic = createTopic[IO](3).unsafeRunSync()

    val data = Map(
      0 -> List("0-0", "0-1"),
      1 -> List("1-0", "1-1"),
      2 -> List("2-0", "2-1")
    )
    val records = data.toList.flatMap {
      case (p, vs) => vs.map(v => new ProducerRecord(topic, p, v, v))
    }
    val tp0 = new TopicPartition(topic, 0)
    val tp1 = new TopicPartition(topic, 1)
    val tp2 = new TopicPartition(topic, 2)

    ProducerApi
      .resource[IO, String, String](BootstrapServers(bootstrapServer))
      .use(
        p =>
          // max.poll.records=1 forces stream to repeat a few times, so we
          // validate the takeThrough predicate
          ConsumerApi
            .resource[IO, String, String](BootstrapServers(bootstrapServer), MaxPollRecords(1))
            .use(
              c =>
                for {
                  _ <- p.sendSyncBatch(records)

                  _ <- c.assign(List(tp0, tp1, tp2))
                  _ <- c.seekToBeginning(List(tp0))
                  _ <- c.seek(tp1, 1)
                  _ <- c.seekToEnd(List(tp2))
                  vs <- c
                    .recordsThroughOffsets(Map(tp0 -> 1, tp1 -> 1, tp2 -> 1), 1 second)
                    .map(_.asScala.map(_.value))
                    .compile
                    .toList

                  // consumer must still be usable after stream halts,
                  // positioned immediately after all of the records it's
                  // already returned
                  _ <- p.sendSync(new ProducerRecord(topic, null, "new"))
                  rs <- c.poll(1 second)

                  _ <- p.close
                  _ <- c.close
                  () = assertEquals(vs.flatten.toSet, Set("0-0", "0-1", "1-1"))
                  () = assertEquals(rs.asScala.map(_.value), List("new"))
                } yield ()
            )
      )
      .unsafeRunSync()
  }

  test("readProcessCommit only commits offsets for successfully processed records") {
    val groupId = unsafeRandomId
    println(s"4 groupId=$groupId")
    val topic = createTopic[IO]().unsafeRunSync()

    //simulates some effect that might fail, e.g. HTTP POST, DB write, etc
    def storeOrFail(values: Ref[IO, Vector[String]], s: String): IO[String] =
      if (Random.nextDouble() < 0.7)
        IO(println(s"success: $s")) *> values.update(_ :+ s).as(s)
      else
        IO(println(s"fail:    $s")) *> IO.raiseError(new RuntimeException("fail"))

    val expected = Vector.range(0, 10).map(_.toString)
    val io = for {
      values <- Ref.of[IO, Vector[String]](Vector.empty)
      _ <- ProducerApi
        .resource[IO, String, String](BootstrapServers(bootstrapServer))
        .use(_.sendSyncBatch(expected.map(s => new ProducerRecord(topic, s, s))))
      consume: Stream[IO, String] = Stream
        .resource(
          ConsumerApi
            .resource[IO, String, String](
              BootstrapServers(bootstrapServer),
              GroupId(groupId),
              AutoOffsetReset.earliest,
              EnableAutoCommit(false)
            )
        )
        .evalTap(_.subscribe(topic))
        .flatMap(_.readProcessCommit(100 millis)(r => storeOrFail(values, r.value))) //only consumes until a failure)
      _ <- consume.attempt.repeat
        .takeThrough(_ != Right(expected.last))
        .compile
        .drain //keeps consuming until last record is successfully processed
      vs <- values.get
    } yield vs
    val actual = io.unsafeRunSync()
    assertEquals(actual, expected) //verifies that no successfully processed record was ever reprocessed
  }

  test("Avro serdes") {
    val groupId = unsafeRandomId
    val topic = createTopic[IO]().unsafeRunSync()

    forAllF { values: Vector[(String, Person)] =>
      val fa = for {
        p <- Stream.resource(
          ProducerApi.Avro.resource[IO, String, Person](
            BootstrapServers(bootstrapServer),
            SchemaRegistryUrl(schemaRegistryUrl)
          )
        )
        c <- Stream.resource(
          ConsumerApi.Avro.Specific.resource[IO, String, Person](
            BootstrapServers(bootstrapServer),
            GroupId(groupId),
            AutoOffsetReset.earliest,
            SchemaRegistryUrl(schemaRegistryUrl)
          )
        )
        v <- writeAndRead(p, c, topic, values)
      } yield v
      fa.compile.toVector.map(assertEquals(_, values))
    }
  }

  //for avro4s tests
  case class PersonId(id: String)
  case class Person2(name: String)
  implicit def personIdRecordFormat = RecordFormat[PersonId]
  implicit def person2RecordFormat = RecordFormat[Person2]

  property("avro4s") {
    val groupId = unsafeRandomId
    println(s"6 groupId=$groupId")
    val topic = createTopic[IO]().unsafeRunSync()

    forAll { values: Vector[(PersonId, Person2)] =>
      val actual = (for {
        p <- Stream.resource(
          ProducerApi.Avro4s.resource[IO, PersonId, Person2](
            BootstrapServers(bootstrapServer),
            SchemaRegistryUrl(schemaRegistryUrl)
          )
        )
        c <- Stream.resource(
          ConsumerApi.Avro4s.resource[IO, PersonId, Person2](
            BootstrapServers(bootstrapServer),
            GroupId(groupId),
            AutoOffsetReset.earliest,
            SchemaRegistryUrl(schemaRegistryUrl)
          )
        )
        v <- writeAndRead(p, c, topic, values)
      } yield v).compile.toVector.unsafeRunSync()
      assertEquals(actual, values)
    }
  }

}
