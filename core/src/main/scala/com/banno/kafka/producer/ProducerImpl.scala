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

import cats.syntax.all._
import cats.effect.Async
import java.util.concurrent.{Future => JFuture}
import scala.jdk.CollectionConverters._
import scala.concurrent.duration._
import org.apache.kafka.common._
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.clients.producer._

case class ProducerImpl[F[_], K, V](p: Producer[K, V])(implicit F: Async[F])
    extends ProducerApi[F, K, V] {
  def abortTransaction: F[Unit] = F.delay(p.abortTransaction())
  def beginTransaction: F[Unit] = F.delay(p.beginTransaction())
  def close: F[Unit] = F.delay(p.close())
  def close(timeout: FiniteDuration): F[Unit] =
    F.delay(p.close(java.time.Duration.ofMillis(timeout.toMillis)))
  def commitTransaction: F[Unit] = F.delay(p.commitTransaction())
  def flush: F[Unit] = F.delay(p.flush())
  def initTransactions: F[Unit] = F.delay(p.initTransactions())
  def metrics: F[Map[MetricName, Metric]] = F.delay(p.metrics().asScala.toMap)
  def partitionsFor(topic: String): F[Seq[PartitionInfo]] =
    F.delay(p.partitionsFor(topic).asScala.toSeq)
  def sendOffsetsToTransaction(
      offsets: Map[TopicPartition, OffsetAndMetadata],
      consumerGroupId: String
  ): F[Unit] =
    F.delay(p.sendOffsetsToTransaction(offsets.asJava, consumerGroupId))

  private[producer] def sendRaw(record: ProducerRecord[K, V]): JFuture[RecordMetadata] =
    p.send(record)
  private[producer] def sendRaw(
      record: ProducerRecord[K, V],
      callback: Callback
  ): JFuture[RecordMetadata] = p.send(record, callback)

  /** Convenience operation that accepts a callback function instead of a Callback instance. */
  private[producer] def sendRaw(
      record: ProducerRecord[K, V],
      callback: Either[Exception, RecordMetadata] => Unit
  ): Unit = {
    sendRaw(record, new Callback() {
      override def onCompletion(rm: RecordMetadata, e: Exception): Unit =
        if (e == null) callback(Right(rm)) else callback(Left(e))
    })
    () //discard the returned JFuture[RecordMetadata]
  }

  /** The returned F[_] completes as soon as the underlying Producer.send(record) call returns.
    * This is immediately after the producer enqueues the record, not after Kafka accepts the write.
    * If the producer's internal queue is full, it will block until the record can be enqueued.
    * The returned F[_] will only contain an error if the producer.send(record) call throws an exception.
    * Using this operation, you will not know when Kafka accepts the write, have no way to access
    * the resulting RecordMetadata, and will not know if the write ultimately fails.
    * This allows for the highest write throughput, and is typically OK.
    * You can still use Producer configs to control write behavior, e.g. enable.idempotence, acks, retries, max.in.flight.requests.per.connection, etc.
    * If your program requires confirmation of record persistence before proceeding, you should call
    * sendSync or sendAsync. */
  def sendAndForget(record: ProducerRecord[K, V]): F[Unit] =
    F.blocking(sendRaw(record)).void //discard the returned JFuture[RecordMetadata]

  /** The returned F[_] completes after Kafka accepts the write, and the RecordMetadata is available.
    * This operation is completely synchronous and blocking: it calls get() on the returned Java Future.
    * The returned F[_] will contain a failure if either the producer.send(record) or the future.get() call throws an exception.
    * You should use this method if your program should not proceed until Kafka accepts the write,
    * or you need to use the RecordMetadata, or you need to explicitly handle any possible error. */
  def sendSync(record: ProducerRecord[K, V]): F[RecordMetadata] =
    F.blocking(sendRaw(record).get())

  /** Similar to sendSync, except the returned F[_] is completed asynchronously, usually on the producer's I/O thread.
   **/
  def sendAsync(record: ProducerRecord[K, V]): F[RecordMetadata] =
    F.async_(sendRaw(record, _))
}

object ProducerImpl {
  //returns the type expected when creating a Resource
  def create[F[_]: Async, K, V](
      p: Producer[K, V]
  ): ProducerApi[F, K, V] =
    ProducerImpl(p)
}
