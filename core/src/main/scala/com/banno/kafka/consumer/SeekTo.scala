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

package com.banno.kafka.consumer

import cats.*
import cats.effect.*
import cats.syntax.all.*

import scala.concurrent.duration.*

import org.apache.kafka.common.*

sealed trait SeekTo

object SeekTo {
  sealed trait Attempt {
    private[SeekTo] def toOffsets[F[_]: Applicative](
        queries: PartitionQueries[F],
        partitions: Iterable[TopicPartition],
    ): F[Map[TopicPartition, Long]]
  }

  object Attempt {
    private case class Timestamps(
        timestamps: Map[TopicPartition, Long]
    ) extends Attempt {
      override def toOffsets[F[_]: Applicative](
          queries: PartitionQueries[F],
          partitions: Iterable[TopicPartition],
      ): F[Map[TopicPartition, Long]] =
        queries.offsetsForTimes(timestamps)
          .map(_.view.mapValues(_.offset).toMap)
    }

    private case class Timestamp(timestamp: Long) extends Attempt {
      override def toOffsets[F[_]: Applicative](
          queries: PartitionQueries[F],
          partitions: Iterable[TopicPartition],
      ): F[Map[TopicPartition, Long]] =
        Timestamps(partitions.map(p => (p, timestamp)).toMap)
          .toOffsets(queries, partitions)
    }

    private object Committed extends Attempt {
      override def toOffsets[F[_]: Applicative](
          queries: PartitionQueries[F],
          partitions: Iterable[TopicPartition],
      ): F[Map[TopicPartition, Long]] =
        queries.committed(partitions.toSet)
          .map(_.view.mapValues(_.offset).toMap)
    }

    private case class Offsets(
        offsets: Map[TopicPartition, Long]
    ) extends Attempt {
      override def toOffsets[F[_]: Applicative](
          queries: PartitionQueries[F],
          partitions: Iterable[TopicPartition],
      ): F[Map[TopicPartition, Long]] =
        offsets.pure
    }

    def timestamps(timestamps: Map[TopicPartition, Long]): Attempt =
      Timestamps(timestamps)

    def timestamp(timestamp: Long): Attempt =
      Timestamp(timestamp)

    val committed: Attempt = Committed

    def offsets(offsets: Map[TopicPartition, Long]): Attempt =
      Offsets(offsets)

    def timestampBeforeNow[F[_]: Clock: Functor](
      duration: FiniteDuration
    ): F[Attempt] =
      Clock[F].realTime.map(now => timestamp(now.toMillis - duration.toMillis))
  }

  sealed trait FinalFallback {
    private[SeekTo] def seek[F[_]](
        consumer: ConsumerApi[F, _, _],
        partitions: Iterable[TopicPartition],
    ): F[Unit]
  }

  object FinalFallback {
    private case object Beginning extends FinalFallback {
      override def seek[F[_]](
          consumer: ConsumerApi[F, _, _],
          partitions: Iterable[TopicPartition],
      ): F[Unit] =
        consumer.seekToBeginning(partitions)
    }
    private case object End extends FinalFallback {
      override def seek[F[_]](
          consumer: ConsumerApi[F, _, _],
          partitions: Iterable[TopicPartition],
      ): F[Unit] =
        consumer.seekToEnd(partitions)
    }

    val beginning: FinalFallback = Beginning
    val end: FinalFallback = End
  }

  private case class Impl(
      attempts: List[Attempt],
      fallback: FinalFallback
  ) extends SeekTo

  private def firstAttemptThen(attempt: Attempt, seekTo: SeekTo): SeekTo =
    seekTo match {
      case s: Impl => s.copy(attempts = attempt :: s.attempts)
    }

  def apply(attempts: Attempt*)(fallback: FinalFallback): SeekTo =
    Impl(attempts.toList, fallback)

  def beginning: SeekTo = Impl(List.empty, FinalFallback.beginning)

  def end: SeekTo = Impl(List.empty, FinalFallback.end)

  def timestamps(timestamps: Map[TopicPartition, Long], default: SeekTo): SeekTo =
    firstAttemptThen(Attempt.timestamps(timestamps), default)

  def timestamp(timestamp: Long, default: SeekTo): SeekTo =
    firstAttemptThen(Attempt.timestamp(timestamp), default)

  def committed(default: SeekTo): SeekTo =
    firstAttemptThen(Attempt.committed, default)

  def offsets(offsets: Map[TopicPartition, Long], default: SeekTo): SeekTo =
    firstAttemptThen(Attempt.offsets(offsets), default)

  def timestampBeforeNow[F[_]: Clock: Functor](
      duration: FiniteDuration,
      default: SeekTo
  ): F[SeekTo] =
    Clock[F].realTime.map(now => timestamp(now.toMillis - duration.toMillis, default))

  def seek[F[_]: Monad](
      consumer: ConsumerApi[F, _, _],
      partitions: Iterable[TopicPartition],
      seekTo: SeekTo
  ): F[Unit] =
    seekTo match {
      case Impl(List(), finalFallback) =>
        finalFallback.seek(consumer, partitions)
      case Impl(attempt :: attempts, default) =>
        partitions.toList.traverse_ { tp =>
          attempt
            .toOffsets[F](consumer.partitionQueries, partitions)
            .flatMap(
              _.get(tp)
              //p could be mapped to an explicit null value
                .flatMap(Option(_))
                .fold(SeekTo.seek(consumer, List(tp), Impl(attempts, default)))(
                  consumer.seek(tp, _)
                )
            )
        }
    }
}
