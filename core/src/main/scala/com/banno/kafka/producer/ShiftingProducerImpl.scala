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

import scala.concurrent.ExecutionContext

case class ShiftingProducerImpl[F[_], K, V](
    p: ProducerApi[F, K, V],
    blockingContext: ExecutionContext
)(implicit CS: ContextShift[F])
    extends ProducerApi[F, K, V] {
  def abortTransaction: F[Unit] = CS.evalOn(blockingContext)(p.abortTransaction)
  def beginTransaction: F[Unit] = CS.evalOn(blockingContext)(p.beginTransaction)
  def close: F[Unit] = CS.evalOn(blockingContext)(p.close)
  def close(timeout: FiniteDuration): F[Unit] = CS.evalOn(blockingContext)(p.close(timeout))
  def commitTransaction: F[Unit] = CS.evalOn(blockingContext)(p.commitTransaction)
  def flush: F[Unit] = CS.evalOn(blockingContext)(p.flush)
  def initTransactions: F[Unit] = CS.evalOn(blockingContext)(p.initTransactions)
  def metrics: F[Map[MetricName, Metric]] = CS.evalOn(blockingContext)(p.metrics)
  def partitionsFor(topic: String): F[Seq[PartitionInfo]] =
    CS.evalOn(blockingContext)(p.partitionsFor(topic))
  def sendOffsetsToTransaction(
      offsets: Map[TopicPartition, OffsetAndMetadata],
      consumerGroupId: String
  ): F[Unit] =
    CS.evalOn(blockingContext)(p.sendOffsetsToTransaction(offsets, consumerGroupId))

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
    CS.evalOn(blockingContext)(p.sendAndForget(record))
  def sendSync(record: ProducerRecord[K, V]): F[RecordMetadata] =
    CS.evalOn(blockingContext)(p.sendSync(record))
  def sendAsync(record: ProducerRecord[K, V]): F[RecordMetadata] =
    CS.evalOn(blockingContext)(p.sendAsync(record))
}
