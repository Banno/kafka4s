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
  override val munitIOTimeout = 90.seconds

  // skip slow tests with,
  //   core/testOnly com.banno.kafka.ProcessingAndCommittingSpec -- --exclude-tags=slow
  val slow = new munit.Tag("slow")

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
      EnableAutoCommit(b = false),
    ) ++ configs.toList
    ConsumerApi.resource[IO, Int, Int](configs2: _*)
  }

  object Offset {
    def unapply(r: Any): Option[Int] =
      r match {
        case m: Map[_, _] =>
          m.toList match {
            case Nil => Some(0)
            case List((_, o: OffsetAndMetadata)) => Some(o.offset.toInt)
            case _ => None
          }
        case _ => None
      }
  }

  def dedupe(os: List[Int]): List[Int] =
    if (os.isEmpty) Nil
    else os.head :: ((os zip os.tail) collect { case (x, y) if x != y => y })

  def checkOffsets(sent: Int, results: List[Any], maxRecordCount: Int): Unit = {
    assert((0 until results.size / 2).forall(i => results(i * 2) == i))

    val offsets = dedupe(
      results.collect { case Offset(o) => o }.dropWhile(_ == 0)
    )
    assertEquals(offsets.last, sent)
    assert(offsets.zipWithIndex.forall { case (o, i) =>
      o == (i + 1) * maxRecordCount
    })
  }

  def checkBatchedOffsets(
      sent: Int,
      results: List[Any],
      batchSize: Int,
      maxRecordCount: Int,
  ): Unit = {
    assert(
      (0 until results.size / 2).forall(i =>
        results(i * 2) == List.iterate(batchSize * i, batchSize)(_ + 1)
      )
    )

    val offsets = dedupe(
      results.collect { case Offset(o) => o }.dropWhile(_ == 0)
    )
    assertEquals(offsets.last, sent)
    assert(offsets.zipWithIndex.forall { case (o, i) =>
      o == (i + 1) * maxRecordCount
    })
  }

  def checkOffsetsFuzzy(
      sent: Int,
      results: List[Any],
      maxRecordCount: Int,
  ): Unit = {
    assert((0 until results.size / 2).forall(i => results(i * 2) == i))

    val offsets = results.collect { case Offset(o) => o }
    assertEquals(offsets.last, sent)
    assert(offsets.zipWithIndex.forall { case (o, i) =>
      (i + 1 - o) < (maxRecordCount + 1)
    })
  }

  def committed(
      consumer: ConsumerApi[IO, Int, Int],
      ps: Set[TopicPartition],
      n: Int,
      finalWait: FiniteDuration = 10.millis,
  ): Stream[IO, Map[TopicPartition, OffsetAndMetadata]] =
    Stream
      .eval(IO.sleep(10.millis) *> consumer.partitionQueries.committed(ps))
      .repeatN(n.toLong - 1) ++
    Stream.sleep_[IO](
      finalWait
    ) ++ // commits are now asynchronous wrt processing so wait to avoid missing the last
    Stream.eval(consumer.partitionQueries.committed(ps))

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
          results <- pac
            .take(values.size.toLong)
            .interleave(committed(consumer, ps, values.size))
            .compile
            .toList
        } yield {
          assertEquals(c0, empty)
          assertEquals(results.size, values.size * 2)

          checkOffsets(10, results, 2)
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
          results <- pac
            .take(values.size.toLong / 2)
            .interleave(committed(consumer, ps, values.size / 2))
            .compile
            .toList
        } yield {
          assertEquals(c0, empty)
          assertEquals(results.size, values.size)

          checkBatchedOffsets(10, results, 2, 2)
        }
      }
    }
  }

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
          results <- pac
            .take(values.size.toLong)
            .interleave(committed(consumer, ps, values.size, 300.millis))
            .compile
            .toList
        } yield {
          assertEquals(c0, empty)
          assertEquals(results.size, values.size * 2)

          checkOffsetsFuzzy(10, results, 2)
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

  test("keepalive stream reads committed offsets on restart") {
    producerResource.use { producer =>
      for {
        groupId <- IO(genGroupId)
        topic <- createTestTopic[IO]()
        p = new TopicPartition(topic, 0)
        ps = Set(p)
        keepAlive = 1.second
        records = (0 to 9).toList.map(v => new ProducerRecord(topic, v, v))
        count = records.size.toLong
        _ <- producer.sendAsyncBatch(records)
        cpause <- consumerResource(GroupId(groupId)).use { consumer =>
          for {
            _ <- consumer.assign(ps)
            c0 <- consumer.partitionQueries.committed(ps)
            pac = consumer.processingAndCommitting(
              pollTimeout = 100.millis,
              maxRecordCount = 1,
              maxElapsedTime = Long.MaxValue.nanos,
              keepAlive,
            )(_.value.pure[IO])
            results <- pac.take(count / 2).compile.toList
            c1 <- consumer.partitionQueries.committed(ps)
          } yield {
            assert(c0.isEmpty)
            assertEquals(results, (0 to 4).toList)
            c1
          }
        }
        crestart <- consumerResource(GroupId(groupId)).use { consumer =>
          for {
            _ <- consumer.assign(ps)
            c0 <- consumer.partitionQueries.committed(ps)
            pac = consumer.processingAndCommitting(
              pollTimeout = 100.millis,
              maxRecordCount = 1,
              maxElapsedTime = Long.MaxValue.nanos,
              keepAlive,
            )(_.value.pure[IO])
            results <- pac.take(count).interruptAfter(100.millis).compile.toList
            c1 <- consumer.partitionQueries.committed(ps)
          } yield {
            assertEquals(results, (5 to 9).toList)
            assertEquals(c0(p).offset, count / 2)
            assertEquals(c1(p).offset, count)
            c0
          }
        }
      } yield {
        assertEquals(cpause(p).offset, count / 2)
        assertEquals(crestart(p).offset, count / 2)
      }
    }
  }

  // skip slow tests with,
  //   core/testOnly com.banno.kafka.ProcessingAndCommittingSpec -- --exclude-tags=slow
  test("offsets expire with no keepalive (slow)".tag(slow)) {
    val expiryTime = 70
    val producerPause = expiryTime + 5
    producerResource.use { producer =>
      for {
        _ <- IO.println(s"slow test >= $producerPause seconds")
        groupId <- IO(genGroupId)
        topic <- createTestTopic[IO]()
        p = new TopicPartition(topic, 0)
        values = (0 to 9).toList
        records = values.map(v => new ProducerRecord[Int, Int](topic, v, v))
        pstream = Stream.emits[IO, ProducerRecord[Int, Int]](
          records.take(5)
        ) ++ Stream.sleep_[IO](producerPause.seconds) ++ Stream
          .emits[IO, ProducerRecord[Int, Int]](records.drop(5))
        _ <- producer.sinkAsync(pstream).compile.drain.start
        _ <-
          consumerResource(GroupId(groupId)).use { consumer =>
            for {
              _ <- consumer.assign(List(p))
              pac = consumer.processingAndCommitting(
                pollTimeout = 100.millis,
                maxRecordCount = 1,
                maxElapsedTime = Long.MaxValue.nanos,
              )(_.value.pure[IO])
              results <- pac
                .evalTap(IO.println)
                .interruptAfter(
                  expiryTime.seconds
                ) // continue until the committed offsets expire
                .take(values.size.toLong)
                .compile
                .toList
              _ <- IO.println("restarting consumer")
            } yield {
              assertEquals(results, List(0, 1, 2, 3, 4))
            }
          }
        _ <-
          consumerResource(GroupId(groupId)).use { consumer =>
            for {
              _ <- IO.println("consumer restarted")
              _ <- consumer.assign(List(p))
              pac = consumer.processingAndCommitting(
                pollTimeout = 100.millis,
                maxRecordCount = 1,
                maxElapsedTime = Long.MaxValue.nanos,
              )(_.value.pure[IO])
              results <- pac
                .evalTap(IO.println)
                .take(values.size.toLong)
                .compile
                .toList
            } yield {
              assertEquals(results, List(0, 1, 2, 3, 4, 5, 6, 7, 8, 9))
            }
          }
      } yield ()
    }
  }

  // skip slow tests with,
  //   core/testOnly com.banno.kafka.ProcessingAndCommittingSpec -- --exclude-tags=slow
  test("offsets preserved with keepalive (slow)".tag(slow)) {
    val expiryTime = 70
    val producerPause = expiryTime + 5
    producerResource.use { producer =>
      for {
        _ <- IO.println(s"slow test >= $producerPause seconds")
        groupId <- IO(genGroupId)
        topic <- createTestTopic[IO]()
        p = new TopicPartition(topic, 0)
        keepAlive = 1.second
        values = (0 to 9).toList
        records = values.map(v => new ProducerRecord[Int, Int](topic, v, v))
        pstream = Stream.emits[IO, ProducerRecord[Int, Int]](
          records.take(5)
        ) ++ Stream.sleep_[IO](producerPause.seconds) ++ Stream
          .emits[IO, ProducerRecord[Int, Int]](records.drop(5))
        _ <- producer.sinkAsync(pstream).compile.drain.start
        _ <-
          consumerResource(GroupId(groupId)).use { consumer =>
            for {
              _ <- consumer.assign(List(p))
              pac = consumer.processingAndCommitting(
                pollTimeout = 100.millis,
                maxRecordCount = 1,
                maxElapsedTime = Long.MaxValue.nanos,
                keepAlive,
              )(_.value.pure[IO])
              results <- pac
                .interruptAfter(
                  expiryTime.seconds
                ) // continue past committed offsets expiry
                .evalTap(IO.println)
                .take(values.size.toLong)
                .compile
                .toList
              _ <- IO.println("restarting consumer")
            } yield {
              assertEquals(results, List(0, 1, 2, 3, 4))
            }
          }
        _ <-
          consumerResource(GroupId(groupId)).use { consumer =>
            for {
              _ <- IO.println("consumer restarted")
              _ <- consumer.assign(List(p))
              pac = consumer.processingAndCommitting(
                pollTimeout = 100.millis,
                maxRecordCount = 1,
                maxElapsedTime = Long.MaxValue.nanos,
              )(_.value.pure[IO])
              results <- pac
                .evalTap(IO.println)
                .take(values.size.toLong / 2)
                .compile
                .toList
            } yield {
              assertEquals(results, List(5, 6, 7, 8, 9))
            }
          }
      } yield ()
    }
  }

  test(
    "keepalive stream commits correct offset after no new records (empty topic)"
  ) {
    consumerResource().use { consumer =>
      for {
        topic <- createTestTopic[IO]()
        p = new TopicPartition(topic, 0)
        ps = Set(p)
        _ <- consumer.assign(ps)
        c0 <- consumer.partitionQueries.committed(ps)
        () <- consumer.commitSync(Map(p -> new OffsetAndMetadata(0)))
        c0b <- consumer.partitionQueries.committed(ps)
        pac = consumer
          .processingAndCommitting(
            pollTimeout = 100.millis,
            maxRecordCount = Long.MaxValue,
            maxElapsedTime = Long.MaxValue.nanos,
            keepAliveInterval = 1.second,
          )(_.value.pure[IO])
          .interruptAfter(3.seconds)
        results <- pac.compile.toList
        c1 <- consumer.partitionQueries.committed(ps)
      } yield {
        assert(c0.isEmpty)
        assertEquals(c0b(p).offset, 0L)
        assert(results.isEmpty)
        assertEquals(c1(p).offset, 0L)
      }
    }
  }

  test(
    "keepalive stream commits correct offset after no new records (non-empty topic)"
  ) {
    producerResource.use { producer =>
      for {
        groupId <- IO(genGroupId)
        topic <- createTestTopic[IO]()
        p = new TopicPartition(topic, 0)
        ps = Set(p)
        keepAlive = 1.second
        records = (0 to 9).toList.map(v => new ProducerRecord(topic, v, v))
        count = records.size.toLong
        _ <- producer.sendAsyncBatch(records)
        cpause <- consumerResource(GroupId(groupId)).use { consumer =>
          for {
            _ <- consumer.assign(ps)
            c0 <- consumer.partitionQueries.committed(ps)
            pac = consumer.processingAndCommitting(
              pollTimeout = 100.millis,
              maxRecordCount = 1,
              maxElapsedTime = Long.MaxValue.nanos,
              keepAlive,
            )(_.value.pure[IO])
            results <- pac.take(count).compile.toList
            c1 <- consumer.partitionQueries.committed(ps)
          } yield {
            assert(c0.isEmpty)
            assertEquals(results, (0 to 9).toList)
            c1
          }
        }
        crestart <- consumerResource(GroupId(groupId)).use { consumer =>
          for {
            _ <- consumer.assign(ps)
            c0 <- consumer.partitionQueries.committed(ps)
            pac = consumer.processingAndCommitting(
              pollTimeout = 100.millis,
              maxRecordCount = 1,
              maxElapsedTime = Long.MaxValue.nanos,
              keepAlive,
            )(_.value.pure[IO])
            results <- pac.take(count).interruptAfter(3.seconds).compile.toList
            c1 <- consumer.partitionQueries.committed(ps)
          } yield {
            assert(results.isEmpty)
            assertEquals(c0(p).offset, count)
            assertEquals(c1(p).offset, count)
            c0
          }
        }
      } yield {
        assertEquals(cpause(p).offset, count)
        assertEquals(crestart(p).offset, count)
      }
    }
  }

}
