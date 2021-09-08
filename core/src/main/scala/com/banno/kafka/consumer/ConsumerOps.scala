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

import cats._
import cats.implicits._
import fs2._
import java.util.ConcurrentModificationException

import cats.effect.{Sync, Clock}
import cats.effect.concurrent.Ref

import scala.jdk.CollectionConverters._
import scala.concurrent.duration._
import org.apache.kafka.common._
import org.apache.kafka.common.errors.WakeupException
import org.apache.kafka.clients.consumer._
import com.banno.kafka._
import fs2.concurrent.{Signal, SignallingRef}
import org.typelevel.log4cats.slf4j.Slf4jLogger

sealed trait SeekTo

object SeekTo {

  private case object Beginning extends SeekTo
  private case object End extends SeekTo
  private case class Timestamps(timestamps: Map[TopicPartition, Long], default: SeekTo)
      extends SeekTo
  private case class Timestamp(timestamp: Long, default: SeekTo) extends SeekTo
  private case class Committed(default: SeekTo) extends SeekTo
  private case class Offsets(offsets: Map[TopicPartition, Long], default: SeekTo) extends SeekTo
  private case class OffsetsBeforeLast(count: Long, commitMarkerAdjustment: Boolean, default: SeekTo) extends SeekTo

  def beginning: SeekTo = Beginning
  def end: SeekTo = End
  def timestamps(timestamps: Map[TopicPartition, Long], default: SeekTo): SeekTo =
    Timestamps(timestamps, default)
  def timestamp(timestamp: Long, default: SeekTo): SeekTo = Timestamp(timestamp, default)
  def committed(default: SeekTo): SeekTo = Committed(default)
  def offsets(offsets: Map[TopicPartition, Long], default: SeekTo): SeekTo =
    Offsets(offsets, default)
  def timestampBeforeNow[F[_]: Clock: Functor](duration: FiniteDuration, default: SeekTo): F[SeekTo] = 
    Clock[F].realTime(MILLISECONDS).map(now => 
      timestamp(now - duration.toMillis, default)
    )
  def offsetsBeforeLast(count: Long, commitMarkerAdjustment: Boolean, default: SeekTo): SeekTo = 
    OffsetsBeforeLast(count, commitMarkerAdjustment, default)

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
          offsets <- consumer.offsetsForTimes(ts)
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
          committed <- consumer.committed(partitions.toSet)
          () <- seek(
            consumer,
            partitions,
            Offsets(committed.view.mapValues(_.offset).toMap, default)
          )
        } yield ()
      case OffsetsBeforeLast(count, commitMarkerAdjustment, default) => 
        for {
          last <- consumer.lastOffsets(partitions, commitMarkerAdjustment)
          () <- seek(consumer, partitions, Offsets(last.view.mapValues(_ - count).toMap, default))
        } yield ()
    }
}

case class ConsumerOps[F[_], K, V](consumer: ConsumerApi[F, K, V]) {

  def log[G[_]: Sync] = Slf4jLogger.getLoggerFromClass(this.getClass)

  def subscribe(topics: String*): F[Unit] =
    consumer.subscribe(topics)

  def partitionsFor(topics: List[String])(implicit F: Applicative[F]): F[List[PartitionInfo]] =
    topics.flatTraverse(consumer.partitionsFor(_).map(_.toList))

  def assign(
      topics: List[String],
      offsets: Map[TopicPartition, Long],
      seekTo: SeekTo = SeekTo.beginning
  )(implicit F: Monad[F]): F[Unit] =
    assignAndSeek(topics, SeekTo.offsets(offsets, seekTo))

  def assign(topic: String, offsets: Map[TopicPartition, Long])(implicit F: Monad[F]): F[Unit] =
    assign(List(topic), offsets)

  def assignAndSeek(
      topics: List[String],
      seekTo: SeekTo,
  )(implicit F: Monad[F]): F[Unit] =
    for {
      infos <- consumer.partitionsFor(topics)
      partitions = infos.map(_.toTopicPartition)
      () <- consumer.assign(partitions)
      () <- SeekTo.seek(consumer, partitions, seekTo)
    } yield ()

  //this doesn't work as-is, because partitions are not assigned when subscribe returns, need to use ConsumerRebalanceListener
  // def subscribeAndSeek(
  //     topics: List[String],
  //     seekTo: SeekTo,
  // )(implicit F: Monad[F]): F[Unit] =
  //   for {
  //     infos <- consumer.partitionsFor(topics)
  //     partitions = infos.map(_.toTopicPartition)
  //     () <- consumer.subscribe(topics)
  //     () <- SeekTo.seek(consumer, partitions, seekTo)
  //   } yield ()

  def positions[G[_]: Traverse](
      partitions: G[TopicPartition]
  )(implicit F: Applicative[F]): F[G[(TopicPartition, Long)]] =
    partitions.traverse(p => consumer.position(p).map(o => (p, o)))

  def assignmentPositions(implicit F: Monad[F]): F[Map[TopicPartition, Long]] =
    consumer.assignment.flatMap(ps => positions(ps.toList)).map(_.toMap)

  def assignmentPositionsAtLeast(
      offsets: Map[TopicPartition, Long]
  )(implicit F: Monad[F]): F[Boolean] =
    assignmentPositions.map(ps => offsets.forall { case (tp, o) => ps.get(tp).exists(_ >= o) })

  def lastOffsets(partitions: Iterable[TopicPartition], commitMarkerAdjustment: Boolean = false)(
      implicit F: Functor[F]
  ): F[Map[TopicPartition, Long]] =
    consumer
      .endOffsets(partitions)
      .map(
        _.view
          .mapValues(v => if (v <= 0) v - 1 else if (commitMarkerAdjustment) v - 2 else v - 1)
          .toMap
      )

  def assignmentLastOffsets(
      commitMarkerAdjustment: Boolean = false
  )(implicit F: Monad[F]): F[Map[TopicPartition, Long]] =
    consumer.assignment.flatMap(ps => lastOffsets(ps, commitMarkerAdjustment))

  def createCaughtUpSignal(lastOffsets: Map[TopicPartition, Long])(
      implicit F: cats.effect.Concurrent[F]
  ): F[(Signal[F, Boolean], ConsumerRecord[K, V] => F[Unit])] =
    consumer.assignmentPositions.map(_.view.mapValues(_ - 1).toMap).flatMap { currentOffsets => //position is next offset consumer will read
      val initiallyCaughtUp = lastOffsets.forall {
        case (tp, o) => currentOffsets.get(tp).exists(_ >= o)
      }
      lastOffsets.toList
        .sortBy(p => (p._1.topic, p._1.partition))
        .traverse_({
          case (tp, o) => log.debug(s"Last offset for ${tp.topic}-${tp.partition} = $o")
        }) *>
        currentOffsets.toList
          .sortBy(p => (p._1.topic, p._1.partition))
          .traverse_({
            case (tp, o) => log.debug(s"Current offset for ${tp.topic}-${tp.partition} = $o")
          }) *>
        log.debug(s"Initially caught up: $initiallyCaughtUp") *>
        Ref.of[F, Map[TopicPartition, Long]](currentOffsets).flatMap { currentOffsetsRef =>
          SignallingRef[F, Boolean](initiallyCaughtUp).map { caughtUpSignal =>
            val update: ConsumerRecord[K, V] => F[Unit] = cr =>
              caughtUpSignal.get.flatMap { caughtUp =>
                if (!caughtUp) {
                  currentOffsetsRef
                    .update(_ + (new TopicPartition(cr.topic, cr.partition) -> cr.offset))
                    .void *>
                    currentOffsetsRef.get.flatMap { m =>
                      m.toList
                        .sortBy(p => (p._1.topic, p._1.partition))
                        .traverse_({
                          case (tp, o) =>
                            log.debug(s"Current offset for ${tp.topic}-${tp.partition} = $o")
                        }) *>
                        (if (lastOffsets.forall { case (tp, o) => m.get(tp).exists(_ >= o) })
                           caughtUpSignal.set(true) *> log
                             .debug(s"Consumer caught up to end offsets")
                         else F.unit)
                    }
                } else F.unit
              }
            (caughtUpSignal, update)
          }
        }
    }

  def createCaughtUpSignal(commitMarkerAdjustment: Boolean = false)(
      implicit F: cats.effect.ConcurrentEffect[F]
  ): F[(fs2.concurrent.Signal[F, Boolean], ConsumerRecord[K, V] => F[Unit])] =
    consumer.assignmentLastOffsets(commitMarkerAdjustment).flatMap(los => createCaughtUpSignal(los))

  def closeAndRecoverConcurrentModificationWithWakeup(
      implicit F: ApplicativeError[F, Throwable]
  ): F[Unit] =
    consumer.close.recoverWith {
      case _: ConcurrentModificationException => consumer.wakeup
    }

  def closeAndRecoverConcurrentModificationWithWakeup(
      timeout: FiniteDuration
  )(implicit F: ApplicativeError[F, Throwable]): F[Unit] =
    consumer.close(timeout).recoverWith {
      case _: ConcurrentModificationException => consumer.wakeup
    }

  def pollAndRecoverWakeupWithClose(
      timeout: FiniteDuration
  )(implicit F: ApplicativeError[F, Throwable]): F[ConsumerRecords[K, V]] =
    consumer.poll(timeout).recoverWith {
      case _: WakeupException =>
        consumer.close *>
          F.pure(new ConsumerRecords(Map.empty.asJava))
    }

  def pollAndRecoverWakeupWithNone(
      timeout: FiniteDuration
  )(implicit F: ApplicativeError[F, Throwable]): F[Option[ConsumerRecords[K, V]]] =
    consumer.poll(timeout).map(_.some).recoverWith {
      case _: WakeupException =>
        consumer.close *>
          none.pure[F]
    }

  /*
  - provide streams that repeatedly call poll()
  - any call to poll can throw WakeupException (among others)
    - will throw if wakeup is called before or during poll
  - poll will return failed F
  - ^^ will lead to stream failing
  - commonly, poll throwing WakeupException means close consumer
  - provide another function that halts the record stream on WakeupException
  - if properly resourced, consumer will then be closed on stream halt
   */

  /** Calls poll repeatedly, each stream element is the entire collection of received records. */
  def recordsStream(pollTimeout: FiniteDuration): Stream[F, ConsumerRecords[K, V]] =
    Stream.repeatEval(consumer.poll(pollTimeout))

  /** Calls poll repeatedly, producing a chunk for each received collection of records. */
  def recordStream(pollTimeout: FiniteDuration): Stream[F, ConsumerRecord[K, V]] =
    recordsStream(pollTimeout).flatMap(
      crs => Stream.chunk(Chunk.indexedSeq(crs.asScala.toIndexedSeq))
    )

  /** Polls for and returns records until reading at least up to the specified final offsets. After the returned stream halts, records will have been returned at least up to,
    * and possibly after, the specified final offsets. The stream will not halt until reaching (at least) the specified offsets. The consumer is not modified,
    * other then calling poll. The consumer should be assigned/subscribed, and seeked to appropriate initial positions, before calling this function. After this stream halts,
    * the consumer may still be used. The next records polled will follow all of the records returned by this stream. One common use of this stream is to
    * initially "catch up" to the last offsets of all assigned topic partitions during program initialization, before doing other things. */
  def recordsThroughOffsets(finalOffsets: Map[TopicPartition, Long], timeout: FiniteDuration)(
      implicit F: MonadError[F, Throwable]
  ): Stream[F, ConsumerRecords[K, V]] =
    Stream.eval(consumer.assignmentPositions.map(_.view.mapValues(_ - 1).toMap)).flatMap {
      initialOffsets => //position is next offset consumer will read, assume already read up to offset before position
        recordsStream(timeout)
          .mapAccumulate(initialOffsets)(
            (currentOffsets, records) => (currentOffsets ++ records.lastOffsets, records)
          )
          .takeThrough {
            case (currentOffsets, _) =>
              finalOffsets.exists { case (tp, o) => currentOffsets.get(tp).fold(true)(_ < o) }
          }
          .map(_._2)
        //once we've read up to final offset for a partition, we could pause it, so poll would stop returning any newer records from it, and then at end we could resume all partitions
        //not sure if pausing is really necessary, because this stream just needs to guarantee its read *up to* final offsets; reading *past* them should be fine
    }

  def recordsThroughAssignmentLastOffsets(
      timeout: FiniteDuration,
      commitMarkerAdjustment: Boolean = false
  )(implicit F: MonadError[F, Throwable]): Stream[F, ConsumerRecords[K, V]] =
    Stream
      .eval(assignmentLastOffsets(commitMarkerAdjustment))
      .flatMap(recordsThroughOffsets(_, timeout))

  def recordsThroughOffsetsOrZeros(
      finalOffsets: Map[TopicPartition, Long],
      pollTimeout: FiniteDuration,
      maxZeroCount: Int
  )(
      implicit
      F: MonadError[F, Throwable]
  ): Stream[F, ConsumerRecords[K, V]] =
    //position is next offset consumer will read, assume already read up to offset before position
    Stream.eval(consumer.assignmentPositions.map(_.view.mapValues(_ - 1).toMap)).flatMap {
      initialOffsets =>
        consumer
          .recordsStream(pollTimeout)
          .mapAccumulate((initialOffsets, 0)) {
            case ((currentOffsets, zeroCount), records) =>
              (
                (
                  currentOffsets ++ records.lastOffsets,
                  if (records.count() == 0) zeroCount + 1 else 0
                ),
                records
              )
          }
          .takeThrough {
            case ((currentOffsets, zeroCount), _) =>
              finalOffsets.exists {
                case (tp, o) => currentOffsets.get(tp).fold(true)(_ < o)
              } && zeroCount < maxZeroCount
          }
          .map(_._2)
      //once we've read up to final offset for a partition, we could pause it, so poll would stop returning any newer records from it, and then at end we could resume all partitions
      //not sure if pausing is really necessary, because this stream just needs to guarantee its read *up to* final offsets; reading *past* them should be fine
    }

  def recordsThroughAssignmentLastOffsetsOrZeros(
      pollTimeout: FiniteDuration,
      maxZeroCount: Int,
      commitMarkerAdjustment: Boolean = false
  )(implicit F: MonadError[F, Throwable]): Stream[F, ConsumerRecords[K, V]] =
    Stream
      .eval(consumer.assignmentLastOffsets(commitMarkerAdjustment))
      .flatMap(recordsThroughOffsetsOrZeros(_, pollTimeout, maxZeroCount))

  /** Returns a sink that synchronously commits received offsets. */
  def commitSink: Pipe[F, Map[TopicPartition, OffsetAndMetadata], Unit] =
    _.evalMap(consumer.commitSync)

  /** Returns a stream that processes records using the specified function, committing offsets for successfully processed records. If the processing function returns a failure,
    * the stream will halt with that failure, and the record's offset will not be committed. This is still at-least-once processing, but records for which the function returns
    * success (and offset commit succeeds) will not be reprocessed after a subsequent failure; only records for which the function returns failure, or offset commits fail, will be reprocessed.
    * In some use cases this pattern is more appropriate than just using auto-offset-commits, since it will not commit offsets for failed records when the consumer is closed, and will likely result
    * in less reprocessing after a failure. The consumer must be configured to disable offset auto-commits. */
  def readProcessCommit[A](pollTimeout: FiniteDuration)(
      process: ConsumerRecord[K, V] => F[A]
  )(implicit F: ApplicativeError[F, Throwable]): Stream[F, A] =
    consumer
      .recordStream(pollTimeout)
      .evalMap(r => process(r) <* consumer.commitSync(r.nextOffset))

}
