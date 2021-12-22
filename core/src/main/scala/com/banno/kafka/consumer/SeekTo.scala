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

  private case object Beginning extends SeekTo
  private case object End extends SeekTo
  private case class Timestamps(timestamps: Map[TopicPartition, Long], default: SeekTo)
      extends SeekTo
  private case class Timestamp(timestamp: Long, default: SeekTo) extends SeekTo
  private case class Committed(default: SeekTo) extends SeekTo
  private case class Offsets(offsets: Map[TopicPartition, Long], default: SeekTo) extends SeekTo

  def beginning: SeekTo = Beginning
  def end: SeekTo = End
  def timestamps(timestamps: Map[TopicPartition, Long], default: SeekTo): SeekTo =
    Timestamps(timestamps, default)
  def timestamp(timestamp: Long, default: SeekTo): SeekTo = Timestamp(timestamp, default)
  def committed(default: SeekTo): SeekTo = Committed(default)
  def offsets(offsets: Map[TopicPartition, Long], default: SeekTo): SeekTo =
    Offsets(offsets, default)
  def timestampBeforeNow[F[_]: Clock: Functor](duration: FiniteDuration, default: SeekTo): F[SeekTo] =
    Clock[F].realTime.map(now =>
      timestamp(now.toMillis - duration.toMillis, default))

  def seek[F[_]: Monad](
      consumer: ConsumerApi[F, _, _],
      partitions: Iterable[TopicPartition],
      seekTo: SeekTo
  ): F[Unit] =
    seekTo match {
      case Beginning =>
        consumer.seekToBeginning(partitions)
      case End =>
        consumer.seekToEnd(partitions)
      case Offsets(offsets, default) =>
        partitions.toList.traverse_(
          tp =>
            offsets
              .get(tp)
              //p could be mapped to an explicit null value
              .flatMap(Option(_))
              .fold(SeekTo.seek(consumer, List(tp), default))(o => consumer.seek(tp, o))
        )
      case Timestamps(ts, default) =>
        for {
          offsets <- consumer.partitionQueries.offsetsForTimes(ts)
          () <- seek(
            consumer,
            partitions,
            Offsets(offsets.view.mapValues(_.offset).toMap, default)
          )
        } yield ()
      case Timestamp(timestamp, default) =>
        val timestamps = partitions.map(p => (p, timestamp)).toMap
        seek(consumer, partitions, Timestamps(timestamps, default))
      case Committed(default) =>
        for {
          committed <- consumer.partitionQueries.committed(partitions.toSet)
          () <- seek(
            consumer,
            partitions,
            Offsets(committed.view.mapValues(_.offset).toMap, default)
          )
        } yield ()
    }
}
