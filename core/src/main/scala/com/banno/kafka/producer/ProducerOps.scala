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
import cats.syntax.all.*
import fs2.*
import org.apache.kafka.common.*
import org.apache.kafka.clients.consumer.*
import org.apache.kafka.clients.producer.*

case class ProducerOps[F[_], K, V](producer: ProducerApi[F, K, V]) {

  def sendAndForgetBatch[G[_]: Foldable](
      records: G[ProducerRecord[K, V]]
  )(implicit F: Applicative[F]): F[Unit] =
    records.traverse_(producer.sendAndForget)

  def sendAsyncBatch[G[_]: Traverse](
      records: G[ProducerRecord[K, V]]
  )(implicit F: Applicative[F]): F[G[RecordMetadata]] =
    records.traverse(producer.sendAsync)

  def pipeAsync: Pipe[F, ProducerRecord[K, V], RecordMetadata] =
    _.evalMap(producer.sendAsync)

  def sink: Pipe[F, ProducerRecord[K, V], Unit] =
    _.evalMap(producer.sendAndForget)

  def sinkAsync: Pipe[F, ProducerRecord[K, V], Unit] =
    pipeAsync.apply(_).void

  def transaction[G[_]: Foldable](
      records: G[ProducerRecord[K, V]]
  )(implicit F: MonadError[F, Throwable]): F[Unit] =
    (for {
      _ <- producer.beginTransaction
      // should be no need to wait for RecordMetadatas or errors, since commitTransaction flushes and throws
      _ <- producer.sendAndForgetBatch(records)
      _ <- producer.commitTransaction
    } yield ()).handleErrorWith(KafkaTransactionError(_, producer))

  def transaction[G[_]: Foldable](
      records: G[ProducerRecord[K, V]],
      offsets: Map[TopicPartition, OffsetAndMetadata],
      groupMetadata: ConsumerGroupMetadata,
  )(implicit F: MonadError[F, Throwable]): F[Unit] =
    (for {
      _ <- producer.beginTransaction
      // should be no need to wait for RecordMetadatas or errors, since commitTransaction flushes and throws
      _ <- sendAndForgetBatch(records)
      _ <- producer.sendOffsetsToTransaction(offsets, groupMetadata)
      _ <- producer.commitTransaction
    } yield ()).handleErrorWith(KafkaTransactionError(_, producer))
}
