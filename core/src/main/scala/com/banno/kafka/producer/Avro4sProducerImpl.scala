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

import java.util.concurrent.{Future => JFuture}
import scala.concurrent.duration._
import org.apache.kafka.common._
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.clients.producer._
import org.apache.avro.generic.GenericRecord
import com.sksamuel.avro4s.ToRecord
import com.banno.kafka._

//this is like Bifunctor[ProducerApi] but is contravariant in both arguments, cats does not seem to have anything like ContravriantBifunctor...

case class Avro4sProducerImpl[F[_], K: ToRecord, V: ToRecord](
    p: ProducerApi[F, GenericRecord, GenericRecord]
) extends ProducerApi[F, K, V] {
  def abortTransaction: F[Unit] = p.abortTransaction
  def beginTransaction: F[Unit] = p.beginTransaction
  def close: F[Unit] = p.close
  def close(timeout: FiniteDuration): F[Unit] = p.close(timeout)
  def commitTransaction: F[Unit] = p.commitTransaction
  def flush: F[Unit] = p.flush
  def initTransactions: F[Unit] = p.initTransactions
  def metrics: F[Map[MetricName, Metric]] = p.metrics
  def partitionsFor(topic: String): F[Seq[PartitionInfo]] = p.partitionsFor(topic)
  def sendOffsetsToTransaction(
      offsets: Map[TopicPartition, OffsetAndMetadata],
      consumerGroupId: String
  ): F[Unit] = p.sendOffsetsToTransaction(offsets, consumerGroupId)

  private[producer] def sendRaw(record: ProducerRecord[K, V]): JFuture[RecordMetadata] =
    p.sendRaw(record.toGenericRecord)
  private[producer] def sendRaw(
      record: ProducerRecord[K, V],
      callback: Callback
  ): JFuture[RecordMetadata] = p.sendRaw(record.toGenericRecord, callback)
  private[producer] def sendRaw(
      record: ProducerRecord[K, V],
      callback: Either[Exception, RecordMetadata] => Unit
  ): Unit =
    p.sendRaw(record.toGenericRecord, callback)

  def sendAndForget(record: ProducerRecord[K, V]): F[Unit] = p.sendAndForget(record.toGenericRecord)
  def sendSync(record: ProducerRecord[K, V]): F[RecordMetadata] = p.sendSync(record.toGenericRecord)
  def sendAsync(record: ProducerRecord[K, V]): F[RecordMetadata] =
    p.sendAsync(record.toGenericRecord)
}
