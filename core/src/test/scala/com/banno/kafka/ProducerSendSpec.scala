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
import cats.effect.IO
import cats.effect.kernel.Ref
import fs2.Stream
import munit.CatsEffectSuite
import org.apache.kafka.clients.producer.*
import com.banno.kafka.producer.*
import com.banno.kafka.consumer.*
import java.util.concurrent.{
  Future => JFuture,
  TimeUnit,
  Executors,
  CompletableFuture,
}
import scala.concurrent.duration.*
import natchez.Trace.Implicits.noop

class ProducerSendSpec extends CatsEffectSuite with KafkaSpec {

  test("send one record") {
    ProducerApi
      .resource[IO, String, String](
        BootstrapServers(bootstrapServer)
      )
      .use { producer =>
        for {
          topic <- createTestTopic[IO]()
          ack <- producer.send(new ProducerRecord(topic, "a", "a"))
          rm <- ack
        } yield {
          assertEquals(rm.topic, topic)
          assertEquals(rm.offset, 0L)
        }
      }
  }

  test("send many records") {
    ProducerApi
      .resource[IO, Int, Int](
        BootstrapServers(bootstrapServer)
      )
      .use { producer =>
        ConsumerApi
          .resource[IO, Int, Int](
            BootstrapServers(bootstrapServer),
            GroupId(genGroupId),
            AutoOffsetReset.earliest,
          )
          .use { consumer =>
            for {
              topic <- createTestTopic[IO]()
              values = (0 to 9).toList
              sends = values
                .map(v => producer.send(new ProducerRecord(topic, v, v)))
              acks <- sends.sequence
              rms <- acks.sequence
              () <- consumer.subscribe(topic)
              records <- consumer
                .recordStream(100.millis)
                .take(values.size.toLong)
                .compile
                .toList
            } yield {
              assertEquals(rms.size, values.size)
              for ((rm, i) <- rms.zipWithIndex) {
                assertEquals(rm.topic, topic)
                assertEquals(rm.offset, i.toLong)
              }
              assertEquals(values, records.map(_.value))
            }
          }
      }
  }

  test("send batch") {
    ProducerApi
      .resource[IO, Int, Int](
        BootstrapServers(bootstrapServer)
      )
      .use { producer =>
        ConsumerApi
          .resource[IO, Int, Int](
            BootstrapServers(bootstrapServer),
            GroupId(genGroupId),
            AutoOffsetReset.earliest,
          )
          .use { consumer =>
            for {
              topic <- createTestTopic[IO]()
              values = (0 to 9).toList
              records = values.map(v => new ProducerRecord(topic, v, v))
              rms <- producer.sendBatch(records)
              () <- consumer.subscribe(topic)
              records <- consumer
                .recordStream(100.millis)
                .take(values.size.toLong)
                .compile
                .toList
            } yield {
              assertEquals(rms.size, values.size)
              for ((rm, i) <- rms.zipWithIndex) {
                assertEquals(rm.topic, topic)
                assertEquals(rm.offset, i.toLong)
              }
              assertEquals(values, records.map(_.value))
            }
          }
      }
  }

  test("send batch with callback") {
    ProducerApi
      .resource[IO, Int, Int](
        BootstrapServers(bootstrapServer)
      )
      .use { producer =>
        ConsumerApi
          .resource[IO, Int, Int](
            BootstrapServers(bootstrapServer),
            GroupId(genGroupId),
            AutoOffsetReset.earliest,
          )
          .use { consumer =>
            for {
              topic <- createTestTopic[IO]()
              values = (0 to 9).toList
              records = values.map(v => new ProducerRecord(topic, v, v))
              ref <- Ref[IO].of(0)
              rms <- producer.sendBatchWithCallbacks(
                records,
                r => ref.update(_ + r.value),
              )
              () <- consumer.subscribe(topic)
              records <- consumer
                .recordStream(100.millis)
                .take(values.size.toLong)
                .compile
                .toList
              sum <- ref.get
            } yield {
              assertEquals(rms.size, values.size)
              for ((rm, i) <- rms.zipWithIndex) {
                assertEquals(rm.topic, topic)
                assertEquals(rm.offset, i.toLong)
                assertEquals(sum, values.sum)
              }
              assertEquals(values, records.map(_.value))
            }
          }
      }
  }

  test("pipe send batch") {
    ProducerApi
      .resource[IO, Int, Int](
        BootstrapServers(bootstrapServer)
      )
      .use { producer =>
        ConsumerApi
          .resource[IO, Int, Int](
            BootstrapServers(bootstrapServer),
            GroupId(genGroupId),
            AutoOffsetReset.earliest,
          )
          .use { consumer =>
            for {
              topic <- createTestTopic[IO]()
              values = (0 to 9).toList
              records = values.map(v => new ProducerRecord(topic, v, v))
              pstream = Stream(records.take(5), records.drop(5)).covary[IO]
              rms <- pstream
                .through(producer.pipeSendBatch)
                .compile
                .toList
                .map(_.flatten)
              () <- consumer.subscribe(topic)
              records <- consumer
                .recordStream(100.millis)
                .take(values.size.toLong)
                .compile
                .toList
            } yield {
              assertEquals(rms.size, values.size)
              for ((rm, i) <- rms.zipWithIndex) {
                assertEquals(rm.topic, topic)
                assertEquals(rm.offset, i.toLong)
              }
              assertEquals(values, records.map(_.value))
            }
          }
      }
  }

  test("pipe send batch implicitly chunked") {
    ProducerApi
      .resource[IO, Int, Int](
        BootstrapServers(bootstrapServer)
      )
      .use { producer =>
        ConsumerApi
          .resource[IO, Int, Int](
            BootstrapServers(bootstrapServer),
            GroupId(genGroupId),
            AutoOffsetReset.earliest,
          )
          .use { consumer =>
            for {
              topic <- createTestTopic[IO]()
              values = (0 to 9).toList
              records = values.map(v => new ProducerRecord(topic, v, v))
              pstream = Stream
                .emits(records.take(5))
                .covary[IO] ++ Stream.emits(records.drop(5)).covary[IO]
              crms <- pstream
                .through(producer.pipeSendBatchChunks)
                .compile
                .foldChunks(List.empty[List[RecordMetadata]]) {
                  case (acc, chunk) => chunk.toList :: acc
                }
              rms = crms.reverse.flatten
              () <- consumer.subscribe(topic)
              records <- consumer
                .recordStream(100.millis)
                .take(values.size.toLong)
                .compile
                .toList
            } yield {
              assertEquals(crms.size, 2)
              assertEquals(rms.size, values.size)
              for ((rm, i) <- rms.zipWithIndex) {
                assertEquals(rm.topic, topic)
                assertEquals(rm.offset, i.toLong)
              }
              assertEquals(values, records.map(_.value))
            }
          }
      }
  }

  test("pipe send batch explicitly chunked") {
    ProducerApi
      .resource[IO, Int, Int](
        BootstrapServers(bootstrapServer)
      )
      .use { producer =>
        ConsumerApi
          .resource[IO, Int, Int](
            BootstrapServers(bootstrapServer),
            GroupId(genGroupId),
            AutoOffsetReset.earliest,
          )
          .use { consumer =>
            for {
              topic <- createTestTopic[IO]()
              values = (0 to 9).toList
              records = values.map(v => new ProducerRecord(topic, v, v))
              pstream = Stream.emits(records).covary[IO]
              crms <- pstream
                .through(producer.pipeSendBatchChunkN(2))
                .compile
                .foldChunks(List.empty[List[RecordMetadata]]) {
                  case (acc, chunk) => chunk.toList :: acc
                }
              rms = crms.reverse.flatten
              () <- consumer.subscribe(topic)
              records <- consumer
                .recordStream(100.millis)
                .take(values.size.toLong)
                .compile
                .toList
            } yield {
              assertEquals(crms.size, 5)
              assertEquals(rms.size, values.size)
              for ((rm, i) <- rms.zipWithIndex) {
                assertEquals(rm.topic, topic)
                assertEquals(rm.offset, i.toLong)
              }
              assertEquals(values, records.map(_.value))
            }
          }
      }
  }

  test("outer effect fails on send throw") {
    val producer =
      ProducerImpl[IO, String, String](ThrowOnSendProducer[String, String]())
    for {
      topic <- createTestTopic[IO]()
      result <- producer.send(new ProducerRecord(topic, "a", "a")).attempt
    } yield {
      assertEquals(result, Left(SendThrowTestException()))
    }
  }

  test("inner effect fails on callback with exception") {
    val producer =
      ProducerImpl[IO, String, String](FailedCallbackProducer[String, String]())
    for {
      topic <- createTestTopic[IO]()
      ack <- producer.send(new ProducerRecord(topic, "a", "a"))
      result <- ack.attempt
    } yield {
      assertEquals(result, Left(CallbackFailureTestException()))
    }
  }

}

case class SendThrowTestException() extends RuntimeException("Send throw test")

case class ThrowOnSendProducer[K, V]() extends Producer[K, V] {
  def send(r: ProducerRecord[K, V], cb: Callback): JFuture[RecordMetadata] =
    throw SendThrowTestException()

  def abortTransaction(): Unit = ???
  def beginTransaction(): Unit = ???
  def close(x$1: java.time.Duration): Unit = ???
  def close(): Unit = ???
  def commitTransaction(): Unit = ???
  def flush(): Unit = ???
  def initTransactions(): Unit = ???
  def metrics(): java.util.Map[
    org.apache.kafka.common.MetricName,
    _ <: org.apache.kafka.common.Metric,
  ] = ???
  def partitionsFor(
      x$1: String
  ): java.util.List[org.apache.kafka.common.PartitionInfo] = ???
  def send(
      x$1: org.apache.kafka.clients.producer.ProducerRecord[K, V]
  ): java.util.concurrent.Future[
    org.apache.kafka.clients.producer.RecordMetadata
  ] = ???
  def sendOffsetsToTransaction(
      x$1: java.util.Map[
        org.apache.kafka.common.TopicPartition,
        org.apache.kafka.clients.consumer.OffsetAndMetadata,
      ],
      x$2: org.apache.kafka.clients.consumer.ConsumerGroupMetadata,
  ): Unit = ???
  def sendOffsetsToTransaction(
      x$1: java.util.Map[
        org.apache.kafka.common.TopicPartition,
        org.apache.kafka.clients.consumer.OffsetAndMetadata,
      ],
      x$2: String,
  ): Unit = ???
}

case class CallbackFailureTestException()
    extends RuntimeException("Callback throw test")

case class FailedCallbackProducer[K, V]() extends Producer[K, V] {
  val scheduler = Executors.newSingleThreadScheduledExecutor()
  def send(r: ProducerRecord[K, V], cb: Callback): JFuture[RecordMetadata] = {
    scheduler.schedule(
      new Runnable() {
        override def run(): Unit =
          cb.onCompletion(null, CallbackFailureTestException())
      },
      100L,
      TimeUnit.MILLISECONDS,
    )
    new CompletableFuture()
  }

  def abortTransaction(): Unit = ???
  def beginTransaction(): Unit = ???
  def close(x$1: java.time.Duration): Unit = ???
  def close(): Unit = ???
  def commitTransaction(): Unit = ???
  def flush(): Unit = ???
  def initTransactions(): Unit = ???
  def metrics(): java.util.Map[
    org.apache.kafka.common.MetricName,
    _ <: org.apache.kafka.common.Metric,
  ] = ???
  def partitionsFor(
      x$1: String
  ): java.util.List[org.apache.kafka.common.PartitionInfo] = ???
  def send(
      x$1: org.apache.kafka.clients.producer.ProducerRecord[K, V]
  ): java.util.concurrent.Future[
    org.apache.kafka.clients.producer.RecordMetadata
  ] = ???
  def sendOffsetsToTransaction(
      x$1: java.util.Map[
        org.apache.kafka.common.TopicPartition,
        org.apache.kafka.clients.consumer.OffsetAndMetadata,
      ],
      x$2: org.apache.kafka.clients.consumer.ConsumerGroupMetadata,
  ): Unit = ???
  def sendOffsetsToTransaction(
      x$1: java.util.Map[
        org.apache.kafka.common.TopicPartition,
        org.apache.kafka.clients.consumer.OffsetAndMetadata,
      ],
      x$2: String,
  ): Unit = ???
}
