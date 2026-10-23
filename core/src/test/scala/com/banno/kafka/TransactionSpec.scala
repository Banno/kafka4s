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
import cats.effect.{Deferred, IO}
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
}
