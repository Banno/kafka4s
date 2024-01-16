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
package avro4s

import cats.effect.*
import cats.syntax.all.*
import com.banno.kafka.consumer.*
import com.banno.kafka.producer.*
import com.sksamuel.avro4s.RecordFormat
import fs2.*
import java.util.ConcurrentModificationException
import natchez.Trace.Implicits.noop
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.scalacheck.*
import org.scalacheck.magnolia.*
import org.scalatest.EitherValues
import org.scalatest.matchers.should.Matchers
import org.scalatest.propspec.AnyPropSpec
import org.scalatestplus.scalacheck.*
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*
import scala.util.Random

class ConsumerAndProducerApiSpec
    extends AnyPropSpec
    with ScalaCheckDrivenPropertyChecks
    with Matchers
    with EitherValues
    with DockerizedKafkaSpec {

  import cats.effect.unsafe.implicits.global

  implicit def noShrink[T]: Shrink[T] = Shrink.shrinkAny

  // Probably don't need to test every single operation; this is just a sanity check that it is all wired up properly

  def producerRecord[K, V](topic: String)(p: (K, V)): ProducerRecord[K, V] =
    new ProducerRecord[K, V](topic, p._1, p._2)

  def writeAndRead[F[_], K, V](
      producer: ProducerApi[F, K, V],
      consumer: ConsumerApi[F, K, V],
      topic: String,
      values: Vector[(K, V)],
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

  property("Producer API sends records and produces metadata") {
    val topic = createTopic()

    forAll { strings: Vector[String] =>
      // TODO what does sendAsyncBatch actually do?
      //  1. send one record and semantically block until metadata received, send next record and semantically block until metadata received, etc
      //  2. or, send all records in batch, then semantically block until all metadatas received
      // I suspect it's #1, which may be useful, but most of the time #2 is really what we want, for max throughput

      val rms = ProducerApi
        .resource[IO, String, String](BootstrapServers(bootstrapServer))
        .use(
          _.sendAsyncBatch(strings.map(s => new ProducerRecord(topic, s, s)))
        )
        .unsafeRunSync()

      rms.size should ===(strings.size): Unit
      rms.forall(_.topic == topic) should ===(true): Unit
      if (strings.size >= 2) {
        rms.last.offset - rms.head.offset should ===(strings.size - 1)
      }
    }
  }

  // KafkaConsumer is not thread-safe; if one thread is calling poll while another concurrently calls close, close will throw ConcurrentModificationException
  property(
    "Simple consumer close fails with ConcurrentModificationException while polling"
  ) {
    val topic = createTopic()
    ConsumerApi.NonShifting
      .resource[IO, String, String](BootstrapServers(bootstrapServer))
      .use(c =>
        for {
          _ <- c.assign(topic, Map.empty[TopicPartition, Long])
          f <- Spawn[IO].start(c.poll(1.second))
          e <- Temporal[IO].sleep(10.millis) *> c.close.attempt
          _ <- f.join
        } yield {
          e.left.value shouldBe a[ConcurrentModificationException]
        }
      )
      .unsafeRunSync()
  }

  // If we shift all operations onto a singleton thread pool then they become sequential, so we can safely call poll and close concurrently without requiring recovery & wakeup
  property("Singleton shifting consumer close while polling") {
    val topic = createTopic()
    ConsumerApi
      .resource[IO, String, String](BootstrapServers(bootstrapServer))
      .allocated
      .map { case (c, close) =>
        for {
          _ <- c.assign(topic, Map.empty[TopicPartition, Long])
          _ <- Spawn[IO].start(c.poll(1 second))
          e <- Temporal[IO].sleep(100 millis) *> close.attempt
        } yield {
          e.toOption.get should ===(())
        }
      }
      .unsafeRunSync()
  }

  property("Producer and Consumer APIs should write and read records") {
    val groupId = genGroupId
    println(s"2 groupId=$groupId")
    val topic = createTopic()

    forAll { values: Vector[(String, String)] =>
      val actual = (for {
        p <- Stream.resource(
          ProducerApi.resource[IO, String, String](
            BootstrapServers(bootstrapServer)
          )
        )
        c <- Stream.resource(
          ConsumerApi.resource[IO, String, String](
            BootstrapServers(bootstrapServer),
            GroupId(groupId),
            AutoOffsetReset.earliest,
          )
        )
        x <- writeAndRead(p, c, topic, values)
      } yield x).compile.toVector.unsafeRunSync()
      actual should ===(values)
    }
  }

  property("read through final offsets") {
    val topic = createTopic(3)

    val data = Map(
      0 -> List("0-0", "0-1"),
      1 -> List("1-0", "1-1"),
      2 -> List("2-0", "2-1"),
    )
    val records = data.toList.flatMap { case (p, vs) =>
      vs.map(v => new ProducerRecord(topic, p, v, v))
    }
    val tp0 = new TopicPartition(topic, 0)
    val tp1 = new TopicPartition(topic, 1)
    val tp2 = new TopicPartition(topic, 2)

    ProducerApi
      .resource[IO, String, String](BootstrapServers(bootstrapServer))
      .use(p =>
        // max.poll.records=1 forces stream to repeat a few times, so we validate the takeThrough predicate
        ConsumerApi
          .resource[IO, String, String](
            BootstrapServers(bootstrapServer),
            MaxPollRecords(1),
          )
          .use(c =>
            for {
              _ <- p.sendAsyncBatch(records)

              _ <- c.assign(List(tp0, tp1, tp2))
              _ <- c.seekToBeginning(List(tp0))
              _ <- c.seek(tp1, 1)
              _ <- c.seekToEnd(List(tp2))
              vs <- c
                .recordsThroughOffsets(
                  Map(tp0 -> 1, tp1 -> 1, tp2 -> 1),
                  1 second,
                )
                .map(_.asScala.map(_.value))
                .compile
                .toList

              // consumer must still be usable after stream halts, positioned immediately after all of the records it's already returned
              _ <- p.sendAsync(new ProducerRecord(topic, null, "new"))
              rs <- c.poll(1 second)

              _ <- p.close
              _ <- c.close
            } yield {
              (vs.flatten should contain)
                .theSameElementsAs(List("0-0", "0-1", "1-1")): Unit
              rs.asScala.map(_.value) should ===(List("new"))
            }
          )
      )
      .unsafeRunSync()
  }

  property(
    "readProcessCommit only commits offsets for successfully processed records"
  ) {
    val groupId = genGroupId
    println(s"4 groupId=$groupId")
    val topic = createTopic()

    // simulates some effect that might fail, e.g. HTTP POST, DB write, etc
    def storeOrFail(values: Ref[IO, Vector[String]], s: String): IO[String] =
      if (Random.nextDouble() < 0.7)
        IO(println(s"success: $s")) *> values.update(_ :+ s).as(s)
      else
        IO(println(s"fail:    $s")) *> IO.raiseError(
          new RuntimeException("fail")
        )

    val expected = Vector.range(0, 10).map(_.toString)
    val io = for {
      values <- Ref.of[IO, Vector[String]](Vector.empty)
      _ <- ProducerApi
        .resource[IO, String, String](BootstrapServers(bootstrapServer))
        .use(
          _.sendAsyncBatch(expected.map(s => new ProducerRecord(topic, s, s)))
        )
      consume: Stream[IO, String] = Stream
        .resource(
          ConsumerApi
            .resource[IO, String, String](
              BootstrapServers(bootstrapServer),
              GroupId(groupId),
              AutoOffsetReset.earliest,
              EnableAutoCommit(false),
            )
        )
        .evalTap(_.subscribe(topic))
        .flatMap(
          _.readProcessCommit(100 millis)(r => storeOrFail(values, r.value))
        ) // only consumes until a failure)
      _ <- consume.attempt.repeat
        .takeThrough(_ != Right(expected.last))
        .compile
        .drain // keeps consuming until last record is successfully processed
      vs <- values.get
    } yield vs
    val actual = io.unsafeRunSync()
    actual should ===(
      expected
    ) // verifies that no successfully processed record was ever reprocessed
  }

  property("Producer transaction works") {
    pending
  }

  property("Avro serdes") {
    val groupId = genGroupId
    println(s"5 groupId=$groupId")
    val topic = createTopic()

    forAll { values: Vector[(String, Person)] =>
      val actual = (for {
        p <- Stream.resource(
          ProducerApi.Avro.resource[IO, String, Person](
            BootstrapServers(bootstrapServer),
            SchemaRegistryUrl(schemaRegistryUrl),
          )
        )
        c <- Stream.resource(
          ConsumerApi.Avro.Specific.resource[IO, String, Person](
            BootstrapServers(bootstrapServer),
            GroupId(groupId),
            AutoOffsetReset.earliest,
            SchemaRegistryUrl(schemaRegistryUrl),
          )
        )
        v <- writeAndRead(p, c, topic, values)
      } yield v).compile.toVector.unsafeRunSync()
      actual should ===(values)
    }
  }

  // for avro4s tests
  case class PersonId(id: String)
  case class Person2(name: String)
  implicit def personIdRecordFormat = RecordFormat[PersonId]
  implicit def person2RecordFormat = RecordFormat[Person2]

  property("avro4s") {
    val groupId = genGroupId
    println(s"6 groupId=$groupId")
    val topic = createTopic()

    forAll { values: Vector[(PersonId, Person2)] =>
      val actual = (for {
        p <- Stream.resource(
          Avro4sProducer.resource[IO, PersonId, Person2](
            BootstrapServers(bootstrapServer),
            SchemaRegistryUrl(schemaRegistryUrl),
          )
        )
        c <- Stream.resource(
          Avro4sConsumer.resource[IO, PersonId, Person2](
            BootstrapServers(bootstrapServer),
            GroupId(groupId),
            AutoOffsetReset.earliest,
            SchemaRegistryUrl(schemaRegistryUrl),
          )
        )
        v <- writeAndRead(p, c, topic, values)
      } yield v).compile.toVector.unsafeRunSync()
      actual should ===(values)
    }
  }

}
