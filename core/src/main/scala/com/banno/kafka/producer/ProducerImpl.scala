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
import org.apache.kafka.clients.consumer._
import org.apache.kafka.clients.producer._
import scala.concurrent.Promise
import scala.util.Try
import natchez.Span
import natchez.Trace

case class ProducerImpl[F[_], K, V](p: Producer[K, V])(implicit
    F: Async[F],
    T: Trace[F],
) extends ProducerApi[F, K, V] {
  def abortTransaction: F[Unit] = F.interruptible(p.abortTransaction())
  def beginTransaction: F[Unit] = F.interruptible(p.beginTransaction())
  def close: F[Unit] = F.interruptible(p.close())
  def close(timeout: FiniteDuration): F[Unit] =
    F.interruptible(p.close(java.time.Duration.ofMillis(timeout.toMillis)))
  def commitTransaction: F[Unit] = F.interruptible(p.commitTransaction())
  def flush: F[Unit] = F.interruptible(p.flush())
  def initTransactions: F[Unit] = F.interruptible(p.initTransactions())
  def metrics: F[Map[MetricName, Metric]] =
    F.delay(p.metrics().asScala.toMap)
  def partitionsFor(topic: String): F[Seq[PartitionInfo]] =
    F.interruptible(p.partitionsFor(topic).asScala.toSeq)
  def sendOffsetsToTransaction(
      offsets: Map[TopicPartition, OffsetAndMetadata],
      groupMetadata: ConsumerGroupMetadata,
  ): F[Unit] =
    F.interruptible(p.sendOffsetsToTransaction(offsets.asJava, groupMetadata))

  private def sendRaw(
      record: ProducerRecord[K, V]
  ): JFuture[RecordMetadata] =
    p.send(record)
  private def sendRaw(
      record: ProducerRecord[K, V],
      callback: Callback,
  ): JFuture[RecordMetadata] = p.send(record, callback)

  /** Convenience operation that accepts a callback function instead of a
    * Callback instance.
    */
  private def sendRaw(
      record: ProducerRecord[K, V],
      callback: Either[Exception, RecordMetadata] => Unit,
  ): F[Option[F[Unit]]] = F.delay {
    val jFuture: JFuture[RecordMetadata] = sendRaw(
      record,
      new Callback() {
        override def onCompletion(rm: RecordMetadata, e: Exception): Unit =
          if (e == null) callback(Right(rm)) else callback(Left(e))
      },
    )
    Some(F.delay(jFuture.cancel(false)).void)
  }

  /** The outer effect sends the record on a blocking context and is cancelable.
    * The inner effect cancels the underlying send.
    */
  private def sendRaw2(
      record: ProducerRecord[K, V],
      callback: Try[RecordMetadata] => Unit,
  ): F[F[Unit]] =
    // KafkaProducer.send should be interruptible via InterruptedException, so use F.interruptible instead of F.blocking or F.delay
    F.interruptible(
      sendRaw(
        record,
        { (rm, e) => callback(Option(e).toLeft(rm).toTry) },
      )
    ).map(jf => F.delay(jf.cancel(true)).void)

  /** The returned F[_] completes as soon as the underlying
    * Producer.send(record) call returns. This is immediately after the producer
    * enqueues the record, not after Kafka accepts the write. If the producer's
    * internal queue is full, it will block until the record can be enqueued.
    * The returned F[_] will only contain an error if the producer.send(record)
    * call throws an exception. Using this operation, you will not know when
    * Kafka accepts the write, have no way to access the resulting
    * RecordMetadata, and will not know if the write ultimately fails. This
    * allows for the highest write throughput, and is typically OK. You can
    * still use Producer configs to control write behavior, e.g.
    * enable.idempotence, acks, retries, max.in.flight.requests.per.connection,
    * etc. If your program requires confirmation of record persistence before
    * proceeding, you should call sendSync or sendAsync.
    */
  def sendAndForget(record: ProducerRecord[K, V]): F[Unit] =
    F.interruptible(sendRaw(record))
      .void // discard the returned JFuture[RecordMetadata]

  /** Like `sendSync`, but blocks a compute thread awaiting the acknowledgement.
    * Do not use.
    */
  @deprecated("Use sendAsync, or send in kafka4s-6.x", "5.0.4")
  def sendSync(record: ProducerRecord[K, V]): F[RecordMetadata] =
    F.interruptible(sendRaw(record))
      .flatMap(jFut => F.interruptible(jFut.get()))

  /** The returned F[_] completes after Kafka accepts the write, and the
    * RecordMetadata is available. The returned F[_] will raise a failure if
    * either the synchronous buffering or write callback fail.
    *
    * You should use this method if your program should not proceed until Kafka
    * accepts the write, or you need to use the RecordMetadata, or you need to
    * explicitly handle any possible error.
    */
  def sendAsync(record: ProducerRecord[K, V]): F[RecordMetadata] =
    F.async(sendRaw(record, _))

  /** The outer effect completes synchronously when the underlying Producer.send
    * call returns. This is immediately after the producer enqueues the record,
    * not after Kafka accepts the write. If the producer's internal queue is
    * full, it will block until the record can be enqueued (i.e. backpressure).
    * The outer effect is executed on a blocking context and is cancelable. The
    * outer effect will only contain an error if the Producer.send call throws
    * an exception. The inner effect completes asynchronously after Kafka
    * acknowledges the write, and the RecordMetadata is available. The inner
    * effect will only contain an error if the write failed. The inner effect is
    * also cancelable. With this operation, user code can react to both the
    * producer's initial buffering of the record to be sent, and the final
    * result of the write (either success or failure).
    */
  def send(record: ProducerRecord[K, V]): F[F[RecordMetadata]] =
    // inspired by https://github.com/fd4s/fs2-kafka/blob/series/3.x/modules/core/src/main/scala/fs2/kafka/KafkaProducer.scala
    T.span("buffer")(
      (
        T.kernel,
        F.delay(Promise[RecordMetadata]())
          .flatMap(promise =>
            sendRaw2(record, promise.complete)
              .map(cancel =>
                F.fromFutureCancelable(
                  F.delay((promise.future, cancel))
                )
              )
          ),
      ).mapN((kernel, ack) =>
        T.span("ack", Span.Options.parentKernel(kernel))(ack)
      )
    )
}

object ProducerImpl {
  // returns the type expected when creating a Resource
  def create[F[_]: Async: Trace, K, V](
      p: Producer[K, V]
  ): ProducerApi[F, K, V] =
    ProducerImpl(p)
}
