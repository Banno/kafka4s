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

import cats.syntax.all.*
import cats.effect.{Deferred, IO, Resource}
import munit.CatsEffectSuite
import org.apache.kafka.clients.producer.*
import com.banno.kafka.producer.*
import com.banno.kafka.consumer.*

import scala.concurrent.duration.*
import natchez.Trace.Implicits.noop

class TransactionSpec extends CatsEffectSuite with KafkaSpec {

  test("cancelled transaction is aborted, and producer remains usable") {
    (
      ProducerApi.resource[IO, Int, Int](
        BootstrapServers(bootstrapServer),
        TransactionalId(randomId),
      ),
      ConsumerApi.resource[IO, Int, Int](
        BootstrapServers(bootstrapServer),
        GroupId(genGroupId),
        AutoOffsetReset.earliest,
        IsolationLevel.ReadCommitted,
      ),
    ).tupled.use { case (producer, consumer) =>
      for {
        topic <- createTestTopic[IO]()
        () <- producer.initTransactions
        cancelledValues = (0 to 4).toList
        committedValues = (5 to 9).toList
        sent <- Deferred[IO, Unit]
        // sends records, then blocks inside the open transaction until cancelled
        fiber <- producer
          .transactionally(
            producer.sendAndForgetBatch(
              cancelledValues.map(v => new ProducerRecord(topic, v, v))
            ) *>
            producer.flush *> sent.complete(()) *> IO.never[Unit]
          )
          .start
        () <- sent.get
        () <- fiber.cancel
        outcome <- fiber.join
        // fails with IllegalStateException if the cancelled transaction was left open
        () <- producer.transactionally(
          producer.sendAndForgetBatch(
            committedValues.map(v => new ProducerRecord(topic, v, v))
          )
        )
        () <- consumer.subscribe(topic)
        records <- consumer
          .recordStream(100.millis)
          .take(committedValues.size.toLong)
          .compile
          .toList
      } yield {
        assert(outcome.isCanceled)
        assertEquals(records.map(_.value), committedValues)
      }
    }
  }

  def producerAndConsumer: Resource[
    IO,
    (TransactionalProducerApi[IO, Int, Int], ConsumerApi[IO, Int, Int]),
  ] =
    (
      TransactionalProducerApi.resource[IO, Int, Int](
        randomId,
        BootstrapServers(bootstrapServer),
      ),
      ConsumerApi.resource[IO, Int, Int](
        BootstrapServers(bootstrapServer),
        GroupId(genGroupId),
        AutoOffsetReset.earliest,
        IsolationLevel.ReadCommitted,
      ),
    ).tupled

  def records(
      topic: String,
      values: List[Int],
  ): List[ProducerRecord[Int, Int]] =
    values.map(v => new ProducerRecord(topic, v, v))

  def readValues(
      consumer: ConsumerApi[IO, Int, Int],
      topic: String,
      n: Int,
  ): IO[List[Int]] =
    consumer.subscribe(topic) *>
    consumer
      .recordStream(100.millis)
      .map(_.value)
      .take(n.toLong)
      .compile
      .toList

  test("transaction commits records") {
    producerAndConsumer.use { case (producer, consumer) =>
      for {
        topic <- createTestTopic[IO]()
        values = (0 to 9).toList
        () <- producer.transaction(_.sendAndForgetBatch(records(topic, values)))
        read <- readValues(consumer, topic, values.size)
      } yield assertEquals(read, values)
    }
  }

  test(
    "cancelled TransactionalProducer transaction is aborted, and producer remains usable"
  ) {
    producerAndConsumer.use { case (producer, consumer) =>
      for {
        topic <- createTestTopic[IO]()
        cancelledValues = (0 to 4).toList
        committedValues = (5 to 9).toList
        sent <- Deferred[IO, Unit]
        // sends records and waits for their acks, so they reach the broker,
        // then blocks inside the open transaction until cancelled
        fiber <- producer
          .transaction(scope =>
            records(topic, cancelledValues)
              .traverse(scope.send)
              .flatMap(_.sequence) *>
            sent.complete(()) *> IO.never[Unit]
          )
          .start
        () <- sent.get
        () <- fiber.cancel
        outcome <- fiber.join
        // fails with IllegalStateException if the cancelled transaction was left open
        () <- producer.transaction(
          _.sendAndForgetBatch(records(topic, committedValues))
        )
        read <- readValues(consumer, topic, committedValues.size)
      } yield {
        assert(outcome.isCanceled)
        assertEquals(read, committedValues)
      }
    }
  }

  test("concurrent transactions run one at a time") {
    producerAndConsumer.use { case (producer, consumer) =>
      val transactionCount = 10
      val recordsPerTransaction = 5
      for {
        topic <- createTestTopic[IO]()
        valuess = (0 until transactionCount).toList.map(t =>
          (0 until recordsPerTransaction).toList.map(t * 100 + _)
        )
        // waiting for acks, then sleeping, keeps each transaction open long
        // enough for the others to try to overlap it
        () <- valuess.parTraverse_(values =>
          producer.transaction(scope =>
            records(topic, values).traverse(scope.send).flatMap(_.sequence) *>
            IO.sleep(50.millis)
          )
        )
        read <- readValues(consumer, topic, valuess.flatten.size)
      } yield {
        // on a single partition, the records of each transaction are contiguous
        assertEquals(
          read.grouped(recordsPerTransaction).toList.sortBy(_.head),
          valuess,
        )
      }
    }
  }

  test("scope cannot be used after its transaction completes") {
    producerAndConsumer.use { case (producer, _) =>
      for {
        topic <- createTestTopic[IO]()
        escaped <- producer.transaction(IO.pure(_))
        result <- escaped.sendAndForget(new ProducerRecord(topic, 0, 0)).attempt
      } yield assertEquals(result, Left(TransactionScopeClosed()))
    }
  }
}
