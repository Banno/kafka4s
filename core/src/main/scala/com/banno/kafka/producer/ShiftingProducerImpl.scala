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

package com.banno.kafka.producer

import cats.effect._
import java.util.concurrent.{Future => JFuture}

import scala.concurrent.duration._
import org.apache.kafka.common._
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.clients.producer._

case class ShiftingProducerImpl[F[_]: Async, K, V](
    p: ProducerApi[F, K, V],
    blockingContext: Blocker
)(implicit CS: ContextShift[F])
    extends ProducerApi[F, K, V] {
  def abortTransaction: F[Unit] = this.blockingContext.blockOn(p.abortTransaction)
  def beginTransaction: F[Unit] = this.blockingContext.blockOn(p.beginTransaction)
  def close: F[Unit] = this.blockingContext.blockOn(p.close)
  def close(timeout: FiniteDuration): F[Unit] = this.blockingContext.blockOn(p.close(timeout))
  def commitTransaction: F[Unit] = this.blockingContext.blockOn(p.commitTransaction)
  def flush: F[Unit] = this.blockingContext.blockOn(p.flush)
  def initTransactions: F[Unit] = this.blockingContext.blockOn(p.initTransactions)
  def metrics: F[Map[MetricName, Metric]] = this.blockingContext.blockOn(p.metrics)
  def partitionsFor(topic: String): F[Seq[PartitionInfo]] =
    this.blockingContext.blockOn(p.partitionsFor(topic))
  def sendOffsetsToTransaction(
      offsets: Map[TopicPartition, OffsetAndMetadata],
      consumerGroupId: String
  ): F[Unit] =
    this.blockingContext.blockOn(p.sendOffsetsToTransaction(offsets, consumerGroupId))

  private[producer] def sendRaw(record: ProducerRecord[K, V]): JFuture[RecordMetadata] =
    p.sendRaw(record)
  private[producer] def sendRaw(
      record: ProducerRecord[K, V],
      callback: Callback
  ): JFuture[RecordMetadata] = p.sendRaw(record, callback)
  private[producer] def sendRaw(
      record: ProducerRecord[K, V],
      callback: Either[Exception, RecordMetadata] => Unit
  ): Unit = p.sendRaw(record, callback)

  def sendAndForget(record: ProducerRecord[K, V]): F[Unit] =
    this.blockingContext.blockOn(p.sendAndForget(record))
  def sendSync(record: ProducerRecord[K, V]): F[RecordMetadata] =
    this.blockingContext.blockOn(p.sendSync(record))
  def sendAsync(record: ProducerRecord[K, V]): F[RecordMetadata] =
    this.blockingContext.blockOn(p.sendAsync(record))
}
