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

  /** Sends all of the records to the producer (synchronously), so the producer
    * may batch them. After all records are sent, asynchronously waits for all
    * acks. Returns the write metadatas, in order. This is the only batch write
    * operation that allows the producer to perform its own batching, while
    * semantically blocking until all writes have succeeded. It maximizes
    * concurrency and producer batching, and also simplicity of usage. Fails if
    * any individual send or ack fails.
    */
  def sendBatch[G[_]: Traverse](
      records: G[ProducerRecord[K, V]]
  )(implicit F: Monad[F]): F[G[RecordMetadata]] = {
    val sends: G[F[F[RecordMetadata]]] = records.map(producer.send)
    for {
      acks <- sends.sequence
      rms <- acks.sequence
    } yield rms
  }

  def sendBatchNonEmpty[G[_]: NonEmptyTraverse](
      records: G[ProducerRecord[K, V]]
  )(implicit F: FlatMap[F]): F[G[RecordMetadata]] = {
    val sends: G[F[F[RecordMetadata]]] = records.map(producer.send)
    for {
      acks <- sends.nonEmptySequence
      rms <- acks.nonEmptySequence
    } yield rms
  }

  def sendBatchWithCallbacks[G[_]: Traverse](
      records: G[ProducerRecord[K, V]],
      onSend: ProducerRecord[K, V] => F[Unit],
  )(implicit F: Monad[F]): F[G[RecordMetadata]] = {
    val sends: G[F[F[RecordMetadata]]] =
      records.map(r => producer.send(r) <* onSend(r))
    for {
      acks <- sends.sequence
      rms <- acks.sequence
    } yield rms
  }

  def sendBatchWithCallbacks[G[_]: NonEmptyTraverse](
      records: G[ProducerRecord[K, V]],
      onSend: ProducerRecord[K, V] => F[Unit],
  )(implicit F: FlatMap[F]): F[G[RecordMetadata]] = {
    val sends: G[F[F[RecordMetadata]]] =
      records.map(r => producer.send(r) <* onSend(r))
    for {
      acks <- sends.nonEmptySequence
      rms <- acks.nonEmptySequence
    } yield rms
  }

  def pipeSync: Pipe[F, ProducerRecord[K, V], RecordMetadata] =
    _.evalMap(producer.sendSync)

  def pipeAsync: Pipe[F, ProducerRecord[K, V], RecordMetadata] =
    _.evalMap(producer.sendAsync)

  def pipeSendBatch[G[_]: Traverse](implicit
      F: Monad[F]
  ): Pipe[F, G[ProducerRecord[K, V]], G[RecordMetadata]] =
    _.evalMap(sendBatch[G])

  def pipeSendBatchNonEmpty[G[_]: NonEmptyTraverse](implicit
      F: FlatMap[F]
  ): Pipe[F, G[ProducerRecord[K, V]], G[RecordMetadata]] =
    _.evalMap(sendBatchNonEmpty[G])

  /** Uses the stream's chunks as batches of records to send to the producer. */
  def pipeSendBatchChunks(implicit
      F: Monad[F]
  ): Pipe[F, ProducerRecord[K, V], RecordMetadata] =
    s =>
      pipeSendBatch[Chunk](Traverse[Chunk], F)(s.chunks).flatMap(Stream.chunk)

  /** Calls chunkN on the input stream, to create chunks of size `n`, and sends
    * those chunks as batches to the producer.
    */
  def pipeSendBatchChunkN(n: Int, allowFewer: Boolean = true)(implicit
      F: Monad[F]
  ): Pipe[F, ProducerRecord[K, V], RecordMetadata] =
    s =>
      pipeSendBatch[Chunk](Traverse[Chunk], F)(s.chunkN(n, allowFewer))
        .flatMap(Stream.chunk)

  def sink: Pipe[F, ProducerRecord[K, V], Unit] =
    _.evalMap(producer.sendAndForget)

  def sinkAsync: Pipe[F, ProducerRecord[K, V], Unit] =
    pipeAsync.apply(_).void

  def sinkSendBatch[G[_]: Traverse](implicit
      F: Monad[F]
  ): Pipe[F, G[ProducerRecord[K, V]], Unit] =
    pipeSendBatch.apply(_).void

  def sinkSendBatch[G[_]: NonEmptyTraverse](implicit
      F: FlatMap[F]
  ): Pipe[F, G[ProducerRecord[K, V]], Unit] =
    pipeSendBatchNonEmpty.apply(_).void

  def sinkSendBatchChunks(implicit
      F: Monad[F]
  ): Pipe[F, ProducerRecord[K, V], Unit] =
    pipeSendBatchChunks.apply(_).void

  def sinkSendBatchChunkN(n: Int, allowFewer: Boolean = true)(implicit
      F: Monad[F]
  ): Pipe[F, ProducerRecord[K, V], Unit] =
    pipeSendBatchChunkN(n, allowFewer).apply(_).void

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
