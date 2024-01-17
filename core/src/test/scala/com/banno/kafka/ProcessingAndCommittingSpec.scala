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
import cats.effect.{IO, Deferred}
import fs2.Stream
import munit.CatsEffectSuite
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import com.banno.kafka.producer.*
import com.banno.kafka.consumer.*
import scala.concurrent.duration.*
import natchez.Trace.Implicits.noop

class ProcessingAndCommittingSpec extends CatsEffectSuite with KafkaSpec {

  def offsets(
      p: TopicPartition,
      o: Long,
  ): Map[TopicPartition, OffsetAndMetadata] =
    Map(p -> new OffsetAndMetadata(o))

  val empty = Map.empty[TopicPartition, OffsetAndMetadata]

  def producerResource =
    ProducerApi
      .resource[IO, Int, Int](
        BootstrapServers(bootstrapServer)
      )

  def consumerResource(configs: (String, AnyRef)*) = {
    val configs2 = List[(String, AnyRef)](
      BootstrapServers(bootstrapServer),
      GroupId(genGroupId),
      AutoOffsetReset.earliest,
      EnableAutoCommit(false),
    ) ++ configs.toList
    ConsumerApi.resource[IO, Int, Int](configs2: _*)
  }

  test("processingAndCommitting commits after number of records") {
    producerResource.use { producer =>
      consumerResource().use { consumer =>
        for {
          topic <- createTestTopic[IO]()
          p = new TopicPartition(topic, 0)
          ps = Set(p)
          values = (0 to 9).toList
          _ <- producer.sendAsyncBatch(
            values.map(v => new ProducerRecord(topic, v, v))
          )
          () <- consumer.subscribe(topic)
          c0 <- consumer.partitionQueries.committed(ps)
          pac = consumer.processingAndCommitting(
            pollTimeout = 100.millis,
            maxRecordCount = 2,
            maxElapsedTime = Long.MaxValue.nanos,
          )(_.value.pure[IO])
          committed = Stream.repeatEval(
            consumer.partitionQueries.committed(ps)
          )
          results <- pac
            .take(values.size.toLong)
            .interleave(committed)
            .compile
            .toList
        } yield {
          assertEquals(c0, empty)
          assertEquals(results.size, values.size * 2)
          // TODO rewrite this to use values, not so hard-coded
          assertEquals(results(0), 0)
          assertEquals(results(1), empty)
          assertEquals(results(2), 1)
          assertEquals(results(3), offsets(p, 2))
          assertEquals(results(4), 2)
          assertEquals(results(5), offsets(p, 2))
          assertEquals(results(6), 3)
          assertEquals(results(7), offsets(p, 4))
          assertEquals(results(8), 4)
          assertEquals(results(9), offsets(p, 4))
          assertEquals(results(10), 5)
          assertEquals(results(11), offsets(p, 6))
          assertEquals(results(12), 6)
          assertEquals(results(13), offsets(p, 6))
          assertEquals(results(14), 7)
          assertEquals(results(15), offsets(p, 8))
          assertEquals(results(16), 8)
          assertEquals(results(17), offsets(p, 8))
          assertEquals(results(18), 9)
          assertEquals(results(19), offsets(p, 10))
        }
      }
    }
  }

  test("processingAndCommittingBatched commits after number of records") {
    producerResource.use { producer =>
      consumerResource("max.poll.records" -> "2").use { consumer =>
        for {
          topic <- createTestTopic[IO]()
          p = new TopicPartition(topic, 0)
          ps = Set(p)
          values = (0 to 9).toList
          _ <- producer.sendAsyncBatch(
            values.map(v => new ProducerRecord(topic, v, v))
          )
          () <- consumer.subscribe(topic)
          c0 <- consumer.partitionQueries.committed(ps)
          pac = consumer.processingAndCommittingBatched(
            pollTimeout = 100.millis,
            maxRecordCount = 2,
            maxElapsedTime = Long.MaxValue.nanos,
          )(_.recordList(topic).map(_.value).pure[IO])
          committed = Stream.repeatEval(
            consumer.partitionQueries.committed(ps)
          )
          results <- pac
            .take(values.size.toLong / 2)
            .interleave(committed)
            .compile
            .toList
        } yield {
          assertEquals(c0, empty)
          assertEquals(results.size, values.size)
          // TODO rewrite this to use values, not so hard-coded
          assertEquals(results(0), List(0, 1))
          assertEquals(results(1), offsets(p, 2))
          assertEquals(results(2), List(2, 3))
          assertEquals(results(3), offsets(p, 4))
          assertEquals(results(4), List(4, 5))
          assertEquals(results(5), offsets(p, 6))
          assertEquals(results(6), List(6, 7))
          assertEquals(results(7), offsets(p, 8))
          assertEquals(results(8), List(8, 9))
          assertEquals(results(9), offsets(p, 10))
        }
      }
    }
  }

  // this has a real danger of becoming a "flaky test" due to its timing assumptions
  test("processingAndCommitting commits after elapsed time") {
    producerResource.use { producer =>
      consumerResource().use { consumer =>
        for {
          topic <- createTestTopic[IO]()
          p = new TopicPartition(topic, 0)
          ps = Set(p)
          values = (0 to 9).toList
          _ <- producer.sendAsyncBatch(
            values.map(v => new ProducerRecord(topic, v, v))
          )
          () <- consumer.subscribe(topic)
          c0 <- consumer.partitionQueries.committed(ps)
          pac = consumer.processingAndCommitting(
            pollTimeout = 100.millis,
            maxRecordCount = Long.MaxValue,
            maxElapsedTime = 200.millis,
          )(r => IO.sleep(101.millis).as(r.value))
          committed = Stream.repeatEval(
            consumer.partitionQueries.committed(ps)
          )
          results <- pac
            .take(values.size.toLong)
            .interleave(committed)
            .compile
            .toList
        } yield {
          assertEquals(c0, empty)
          assertEquals(results.size, values.size * 2)
          // TODO rewrite this to use values, not so hard-coded
          assertEquals(results(0), 0)
          assertEquals(results(1), empty)
          assertEquals(results(2), 1)
          assertEquals(results(3), offsets(p, 2))
          assertEquals(results(4), 2)
          assertEquals(results(5), offsets(p, 2))
          assertEquals(results(6), 3)
          assertEquals(results(7), offsets(p, 4))
          assertEquals(results(8), 4)
          assertEquals(results(9), offsets(p, 4))
          assertEquals(results(10), 5)
          assertEquals(results(11), offsets(p, 6))
          assertEquals(results(12), 6)
          assertEquals(results(13), offsets(p, 6))
          assertEquals(results(14), 7)
          assertEquals(results(15), offsets(p, 8))
          assertEquals(results(16), 8)
          assertEquals(results(17), offsets(p, 8))
          assertEquals(results(18), 9)
          assertEquals(results(19), offsets(p, 10))
        }
      }
    }
  }

  case class CommitOnFailureException()
      extends RuntimeException("Commit on failure exception")

  test("on failure, commits successful offsets, but not the failed offset") {
    producerResource.use { producer =>
      consumerResource().use { consumer =>
        for {
          topic <- createTestTopic[IO]()
          p = new TopicPartition(topic, 0)
          ps = Set(p)
          values = (0 to 9).toList
          throwOn = 7
          _ <- producer.sendAsyncBatch(
            values.map(v => new ProducerRecord(topic, v, v))
          )
          () <- consumer.subscribe(topic)
          c0 <- consumer.partitionQueries.committed(ps)
          pac = consumer.processingAndCommitting(
            pollTimeout = 100.millis,
            maxRecordCount = Long.MaxValue,
            maxElapsedTime = Long.MaxValue.nanos,
          ) { r =>
            val v = r.value
            if (v == throwOn)
              IO.raiseError(CommitOnFailureException())
            else
              v.pure[IO]
          }
          results <- pac.compile.toList.attempt
          c1 <- consumer.partitionQueries.committed(ps)
        } yield {
          assertEquals(c0, empty)
          assertEquals(results, Left(CommitOnFailureException()))
          // on failure, the committed offset should be the one that failed, so processing will resume there next time and try again
          assertEquals(c1, offsets(p, throwOn.toLong))
        }
      }
    }
  }

  test(
    "on failure, batched commits successful offsets, but not the failed batch offsets"
  ) {
    producerResource.use { producer =>
      consumerResource("max.poll.records" -> "2").use { consumer =>
        for {
          topic <- createTestTopic[IO]()
          p = new TopicPartition(topic, 0)
          ps = Set(p)
          values = (0 to 9).toList
          throwOn = 7
          _ <- producer.sendAsyncBatch(
            values.map(v => new ProducerRecord(topic, v, v))
          )
          () <- consumer.subscribe(topic)
          c0 <- consumer.partitionQueries.committed(ps)
          pac = consumer.processingAndCommittingBatched(
            pollTimeout = 100.millis,
            maxRecordCount = Long.MaxValue,
            maxElapsedTime = Long.MaxValue.nanos,
          ) { rs =>
            val vs = rs.recordList(topic).map(_.value)
            if (vs contains throwOn)
              IO.raiseError(CommitOnFailureException())
            else
              vs.pure[IO]
          }
          results <- pac.compile.toList.attempt
          c1 <- consumer.partitionQueries.committed(ps)
        } yield {
          assertEquals(c0, empty)
          assertEquals(results, Left(CommitOnFailureException()))
          // on failure, the committed offset should be the beginning of the batch that failed, so processing will resume there next time and try again
          assertEquals(c1, offsets(p, 6L))
        }
      }
    }
  }

  test("commits offsets on successful stream finalization") {
    producerResource.use { producer =>
      consumerResource().use { consumer =>
        for {
          topic <- createTestTopic[IO]()
          p = new TopicPartition(topic, 0)
          ps = Set(p)
          values = (0 to 9).toList
          _ <- producer.sendAsyncBatch(
            values.map(v => new ProducerRecord(topic, v, v))
          )
          () <- consumer.subscribe(topic)
          c0 <- consumer.partitionQueries.committed(ps)
          pac = consumer.processingAndCommitting(
            pollTimeout = 100.millis,
            maxRecordCount = Long.MaxValue,
            maxElapsedTime = Long.MaxValue.nanos,
          )(_.value.pure[IO])
          results <- pac.take(values.size.toLong).compile.toList
          c1 <- consumer.partitionQueries.committed(ps)
        } yield {
          assertEquals(c0, empty)
          assertEquals(results, values)
          assertEquals(c1, offsets(p, 10))
        }
      }
    }
  }

  test("commits offsets on stream cancel") {
    producerResource.use { producer =>
      consumerResource().use { consumer =>
        for {
          topic <- createTestTopic[IO]()
          p = new TopicPartition(topic, 0)
          ps = Set(p)
          values = (0 to 9).toList
          cancelOn = 5
          _ <- producer.sendAsyncBatch(
            values.map(v => new ProducerRecord(topic, v, v))
          )
          () <- consumer.subscribe(topic)
          c0 <- consumer.partitionQueries.committed(ps)
          cancelSignal <- Deferred[IO, Unit]
          pac = consumer.processingAndCommitting(
            pollTimeout = 100.millis,
            maxRecordCount = Long.MaxValue,
            maxElapsedTime = Long.MaxValue.nanos,
          ) { r =>
            val v = r.value
            if (v < cancelOn)
              v.pure[IO]
            else
              cancelSignal.complete(()) *> IO.never
          }
          fiber <- pac.compile.toList.start
          () <- cancelSignal.get *> fiber.cancel
          outcome <- fiber.join
          c1 <- consumer.partitionQueries.committed(ps)
        } yield {
          assertEquals(c0, empty)
          assertEquals(outcome.isCanceled, true)
          // since the processing of the cancelOn value never completes, the last successfully
          // processed record was cancelOn - 1, and cancelOn should be the
          // committed offset, so that it is the first one processed after a restart
          assertEquals(c1, offsets(p, cancelOn.toLong))
        }
      }
    }
  }

}
