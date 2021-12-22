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
  sealed trait Attempt

  private case class Timestamps(timestamps: Map[TopicPartition, Long]) extends Attempt
  private case class Timestamp(timestamp: Long) extends Attempt
  private object Committed extends Attempt
  private case class Offsets(offsets: Map[TopicPartition, Long]) extends Attempt

  sealed trait FinalFallback

  private case object Beginning extends FinalFallback
  private case object End extends FinalFallback

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

  def beginning: SeekTo = Impl(List.empty, Beginning)

  def end: SeekTo = Impl(List.empty, End)

  def timestamps(timestamps: Map[TopicPartition, Long], default: SeekTo): SeekTo =
    firstAttemptThen(Timestamps(timestamps), default)

  def timestamp(timestamp: Long, default: SeekTo): SeekTo =
    firstAttemptThen(Timestamp(timestamp), default)

  def committed(default: SeekTo): SeekTo =
    firstAttemptThen(Committed, default)

  def offsets(offsets: Map[TopicPartition, Long], default: SeekTo): SeekTo =
    firstAttemptThen(Offsets(offsets), default)

  def timestampBeforeNow[F[_]: Clock: Functor](duration: FiniteDuration, default: SeekTo): F[SeekTo] =
    Clock[F].realTime.map(now =>
      timestamp(now.toMillis - duration.toMillis, default))

  def seek[F[_]: Monad](
      consumer: ConsumerApi[F, _, _],
      partitions: Iterable[TopicPartition],
      seekTo: SeekTo
  ): F[Unit] =
    seekTo match {
      case Impl(List(), Beginning) =>
        consumer.seekToBeginning(partitions)
      case Impl(List(), End) =>
        consumer.seekToEnd(partitions)
      case Impl(Offsets(offsets) :: attempts, default) =>
        partitions.toList.traverse_(
          tp =>
            offsets
              .get(tp)
              //p could be mapped to an explicit null value
              .flatMap(Option(_))
              .fold(SeekTo.seek(consumer, List(tp), Impl(attempts, default)))(o => consumer.seek(tp, o))
        )
      case Impl(Timestamps(ts) :: attempts, default) =>
        for {
          offsets <- consumer.partitionQueries.offsetsForTimes(ts)
          () <- seek(
            consumer,
            partitions,
            Impl(Offsets(offsets.view.mapValues(_.offset).toMap) :: attempts, default)
          )
        } yield ()
      case Impl(Timestamp(timestamp) :: attempts, default) =>
        val timestamps = partitions.map(p => (p, timestamp)).toMap
        seek(consumer, partitions, Impl(Timestamps(timestamps) :: attempts, default))
      case Impl(Committed :: attempts, default) =>
        for {
          committed <- consumer.partitionQueries.committed(partitions.toSet)
          () <- seek(
            consumer,
            partitions,
            Impl(Offsets(committed.view.mapValues(_.offset).toMap) :: attempts, default)
          )
        } yield ()
    }
}
