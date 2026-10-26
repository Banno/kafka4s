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

import cats.*
import cats.effect.*
import cats.effect.std.Mutex
import cats.syntax.all.*
import com.banno.kafka.*
import natchez.Trace
import org.apache.kafka.clients.consumer.*
import org.apache.kafka.clients.producer.*
import org.apache.kafka.common.*
import org.apache.kafka.common.serialization.Serializer

/** A producer which writes within Kafka transactions.
  *
  * A Kafka producer can have at most one transaction open at a time. Using
  * `TransactionalProducer`, records can only be sent via the
  * [[TransactionScope]] passed to the `body` supplied to `transaction`.
  * Concurrent calls to `transaction` run sequentially. To run transactions in
  * parallel, use several `TransactionalProducers` configured with distinct
  * transactional ids.
  *
  * If `transaction` fails, the underlying Kafka producer is permanently
  * unusable: all later `transaction` calls fail immediately, without evaluating
  * their `body`. To recover, release the `Resource` this producer was acquired
  * from and acquire a new one.
  */
trait TransactionalProducerApi[F[_], K, V] {

  /** Runs `body` within a transaction, which is committed iff `body` succeeds,
    * and aborted if `body` fails or is canceled. Fails with
    * [[TransactionAborted]] or [[FatalError]], as described in
    * [[KafkaTransactionError]].
    */
  def transaction[A](body: TransactionScope[F, K, V] => F[A]): F[A]

  def metrics: F[Map[MetricName, Metric]]
  def partitionsFor(topic: String): F[Seq[PartitionInfo]]
}

/** Sends records within a single transaction. Fails with
  * [[TransactionScopeClosed]] if used after the `body` it was passed to has
  * completed.
  */
trait TransactionScope[F[_], K, V] {
  def sendAndForget(record: ProducerRecord[K, V]): F[Unit]
  def send(record: ProducerRecord[K, V]): F[F[RecordMetadata]]
  def sendOffsetsToTransaction(
      offsets: Map[TopicPartition, OffsetAndMetadata],
      groupMetadata: ConsumerGroupMetadata,
  ): F[Unit]

  final def sendAndForgetBatch[G[_]: Foldable](
      records: G[ProducerRecord[K, V]]
  )(implicit F: Applicative[F]): F[Unit] =
    records.traverse_(sendAndForget)
}

final case class TransactionScopeClosed()
    extends IllegalStateException(
      "TransactionScope used after its transaction completed"
    )

object TransactionalProducerApi {

  def resource[F[_]: Async: Trace, K, V](
      keySerializer: Serializer[K],
      valueSerializer: Serializer[V],
      transactionalId: String,
      configs: (String, AnyRef)*
  ): Resource[F, TransactionalProducerApi[F, K, V]] =
    ProducerApi
      .resource[F, K, V](
        keySerializer,
        valueSerializer,
        configs :+ TransactionalId(transactionalId): _*
      )
      .evalMap(fromProducer(_))

  def resource[F[_]: Async: Trace, K: Serializer, V: Serializer](
      transactionalId: String,
      configs: (String, AnyRef)*
  ): Resource[F, TransactionalProducerApi[F, K, V]] =
    resource[F, K, V](
      implicitly[Serializer[K]],
      implicitly[Serializer[V]],
      transactionalId,
      configs: _*
    )

  /** Initializes transactions on `producer`, which must have been created with
    * a transactional id, and takes ownership of it: `producer` must not be used
    * other than through the result, nor shared with another
    * TransactionalProducer.
    */
  def fromProducer[F[_]: Concurrent, K, V](
      producer: ProducerApi[F, K, V]
  ): F[TransactionalProducerApi[F, K, V]] =
    (producer.initTransactions *> Mutex[F]).map(lock =>
      new TransactionalProducerApi[F, K, V] {
        def transaction[A](body: TransactionScope[F, K, V] => F[A]): F[A] =
          lock.lock.surround(
            Ref[F]
              .of(true)
              .flatMap(open =>
                producer.transactionally(
                  Concurrent[F].guarantee(
                    body(mkScope(producer, open)),
                    open.set(false),
                  )
                )
              )
          )
        def metrics: F[Map[MetricName, Metric]] = producer.metrics
        def partitionsFor(topic: String): F[Seq[PartitionInfo]] =
          producer.partitionsFor(topic)
      }
    )

  private def mkScope[F[_], K, V](
      producer: ProducerApi[F, K, V],
      open: Ref[F, Boolean],
  )(implicit
      F: MonadThrow[F]
  ): TransactionScope[F, K, V] =
    new TransactionScope[F, K, V] {
      def sendAndForget(record: ProducerRecord[K, V]): F[Unit] =
        ensureOpen *> producer.sendAndForget(record)
      def send(record: ProducerRecord[K, V]): F[F[RecordMetadata]] =
        ensureOpen *> producer.send(record)
      def sendOffsetsToTransaction(
          offsets: Map[TopicPartition, OffsetAndMetadata],
          groupMetadata: ConsumerGroupMetadata,
      ): F[Unit] =
        ensureOpen *> producer.sendOffsetsToTransaction(offsets, groupMetadata)

      def ensureOpen: F[Unit] =
        open.get.flatMap(F.raiseError[Unit](TransactionScopeClosed()).unlessA)
    }
}
