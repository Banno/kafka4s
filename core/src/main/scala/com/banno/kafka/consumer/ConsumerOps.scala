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

package com.banno.kafka
package consumer

import cats.*
import cats.effect.*
import cats.effect.implicits.*
import cats.effect.std.{Queue, Semaphore}
import cats.syntax.all.*
import fs2.*

import scala.jdk.CollectionConverters.*
import scala.concurrent.duration.*
import org.apache.kafka.common.*
import org.apache.kafka.common.errors.WakeupException
import org.apache.kafka.clients.consumer.*
import fs2.concurrent.{Signal, SignallingRef}
import org.typelevel.log4cats.slf4j.Slf4jLogger

case class PartitionQueriesOps[F[_]](consumer: PartitionQueries[F]) {

  def partitionsFor(topics: List[String])(implicit
      F: Applicative[F]
  ): F[List[PartitionInfo]] =
    topics.flatTraverse(consumer.partitionsFor(_).map(_.toList))

  def lastOffsets(
      partitions: Iterable[TopicPartition],
      commitMarkerAdjustment: Boolean = false,
  )(implicit
      F: Functor[F]
  ): F[Map[TopicPartition, Long]] =
    consumer
      .endOffsets(partitions)
      .map(
        _.view
          .mapValues(v =>
            if (v <= 0) v - 1 else if (commitMarkerAdjustment) v - 2 else v - 1
          )
          .toMap
      )

}

case class ConsumerOps[F[_], K, V](consumer: ConsumerApi[F, K, V]) {

  def log[G[_]: Sync] = Slf4jLogger.getLoggerFromClass(this.getClass)

  def subscribe(topics: String*): F[Unit] =
    consumer.subscribe(topics)

  def partitionsFor(topics: List[String])(implicit
      F: Applicative[F]
  ): F[List[PartitionInfo]] =
    consumer.partitionQueries.partitionsFor(topics)

  def assign(
      topics: List[String],
      offsets: Map[TopicPartition, Long],
      seekTo: SeekTo = SeekTo.beginning,
  )(implicit F: MonadThrow[F], C: Clock[F]): F[Unit] =
    assignAndSeek(topics, SeekTo.offsets(offsets, seekTo))

  def assign(topic: String, offsets: Map[TopicPartition, Long])(implicit
      F: MonadThrow[F],
      C: Clock[F],
  ): F[Unit] =
    assign(List(topic), offsets)

  def assignAndSeek(
      topics: List[String],
      seekTo: SeekTo,
  )(implicit F: MonadThrow[F], C: Clock[F]): F[Unit] =
    for {
      infos <- consumer.partitionsFor(topics)
      partitions = infos.map(_.toTopicPartition)
      () <- consumer.assign(partitions)
      () <- SeekTo.seek(consumer, partitions, seekTo)
    } yield ()

  // this doesn't work as-is, because partitions are not assigned when subscribe returns, need to use ConsumerRebalanceListener
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
    assignmentPositions.map(ps =>
      offsets.forall { case (tp, o) => ps.get(tp).exists(_ >= o) }
    )

  def lastOffsets(
      partitions: Iterable[TopicPartition],
      commitMarkerAdjustment: Boolean = false,
  )(implicit
      F: Functor[F]
  ): F[Map[TopicPartition, Long]] =
    consumer.partitionQueries.lastOffsets(partitions, commitMarkerAdjustment)

  def assignmentLastOffsets(
      commitMarkerAdjustment: Boolean = false
  )(implicit F: Monad[F]): F[Map[TopicPartition, Long]] =
    consumer.assignment.flatMap(ps => lastOffsets(ps, commitMarkerAdjustment))

  def createCaughtUpSignal(lastOffsets: Map[TopicPartition, Long])(implicit
      F: Async[F]
  ): F[(Signal[F, Boolean], ConsumerRecord[K, V] => F[Unit])] =
    consumer.assignmentPositions.map(_.view.mapValues(_ - 1).toMap).flatMap {
      currentOffsets => // position is next offset consumer will read
        val initiallyCaughtUp = lastOffsets.forall { case (tp, o) =>
          currentOffsets.get(tp).exists(_ >= o)
        }
        lastOffsets.toList
          .sortBy(p => (p._1.topic, p._1.partition))
          .traverse_({ case (tp, o) =>
            log.debug(s"Last offset for ${tp.topic}-${tp.partition} = $o")
          }) *>
        currentOffsets.toList
          .sortBy(p => (p._1.topic, p._1.partition))
          .traverse_({ case (tp, o) =>
            log.debug(s"Current offset for ${tp.topic}-${tp.partition} = $o")
          }) *>
        log.debug(s"Initially caught up: $initiallyCaughtUp") *>
        Ref.of[F, Map[TopicPartition, Long]](currentOffsets).flatMap {
          currentOffsetsRef =>
            SignallingRef[F, Boolean](initiallyCaughtUp).map { caughtUpSignal =>
              val update: ConsumerRecord[K, V] => F[Unit] = cr =>
                caughtUpSignal.get.flatMap { caughtUp =>
                  if (!caughtUp) {
                    currentOffsetsRef
                      .update(
                        _ + (new TopicPartition(
                          cr.topic,
                          cr.partition,
                        ) -> cr.offset)
                      )
                      .void *>
                    currentOffsetsRef.get.flatMap { m =>
                      m.toList
                        .sortBy(p => (p._1.topic, p._1.partition))
                        .traverse_({ case (tp, o) =>
                          log.debug(
                            s"Current offset for ${tp.topic}-${tp.partition} = $o"
                          )
                        }) *>
                      (if (
                         lastOffsets.forall { case (tp, o) =>
                           m.get(tp).exists(_ >= o)
                         }
                       )
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

  def createCaughtUpSignal(commitMarkerAdjustment: Boolean = false)(implicit
      F: Async[F]
  ): F[(Signal[F, Boolean], ConsumerRecord[K, V] => F[Unit])] =
    consumer
      .assignmentLastOffsets(commitMarkerAdjustment)
      .flatMap(los => createCaughtUpSignal(los))

  def pollAndRecoverWakeupWithClose(
      timeout: FiniteDuration
  )(implicit F: ApplicativeError[F, Throwable]): F[ConsumerRecords[K, V]] =
    consumer.poll(timeout).recoverWith { case _: WakeupException =>
      consumer.close *>
      F.pure(new ConsumerRecords(Map.empty.asJava))
    }

  def pollAndRecoverWakeupWithNone(
      timeout: FiniteDuration
  )(implicit
      F: ApplicativeError[F, Throwable]
  ): F[Option[ConsumerRecords[K, V]]] =
    consumer.poll(timeout).map(_.some).recoverWith { case _: WakeupException =>
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

  /** Calls poll repeatedly, each stream element is the entire collection of
    * received records.
    */
  def recordsStream(
      pollTimeout: FiniteDuration
  ): Stream[F, ConsumerRecords[K, V]] =
    Stream.repeatEval(consumer.poll(pollTimeout))

  /** Calls poll repeatedly, producing a chunk for each received collection of
    * records.
    */
  def recordStream(
      pollTimeout: FiniteDuration
  ): Stream[F, ConsumerRecord[K, V]] =
    recordsStream(pollTimeout).flatMap(crs =>
      Stream.chunk(Chunk.from(crs.asScala.toIndexedSeq))
    )

  /** Polls for and returns records until reading at least up to the specified
    * final offsets. After the returned stream halts, records will have been
    * returned at least up to, and possibly after, the specified final offsets.
    * The stream will not halt until reaching (at least) the specified offsets.
    * The consumer is not modified, other then calling poll. The consumer should
    * be assigned/subscribed, and seeked to appropriate initial positions,
    * before calling this function. After this stream halts, the consumer may
    * still be used. The next records polled will follow all of the records
    * returned by this stream. One common use of this stream is to initially
    * "catch up" to the last offsets of all assigned topic partitions during
    * program initialization, before doing other things.
    */
  def recordsThroughOffsets(
      finalOffsets: Map[TopicPartition, Long],
      timeout: FiniteDuration,
  )(implicit
      F: MonadError[F, Throwable]
  ): Stream[F, ConsumerRecords[K, V]] =
    Stream
      .eval(consumer.assignmentPositions.map(_.view.mapValues(_ - 1).toMap))
      .flatMap {
        initialOffsets => // position is next offset consumer will read, assume already read up to offset before position
          recordsStream(timeout)
            .mapAccumulate(initialOffsets)((currentOffsets, records) =>
              (currentOffsets ++ records.lastOffsets, records)
            )
            .takeThrough { case (currentOffsets, _) =>
              finalOffsets.exists { case (tp, o) =>
                currentOffsets.get(tp).fold(true)(_ < o)
              }
            }
            .map(_._2)
          // once we've read up to final offset for a partition, we could pause it, so poll would stop returning any newer records from it, and then at end we could resume all partitions
          // not sure if pausing is really necessary, because this stream just needs to guarantee its read *up to* final offsets; reading *past* them should be fine
      }

  def recordsThroughAssignmentLastOffsets(
      timeout: FiniteDuration,
      commitMarkerAdjustment: Boolean = false,
  )(implicit F: MonadError[F, Throwable]): Stream[F, ConsumerRecords[K, V]] =
    Stream
      .eval(assignmentLastOffsets(commitMarkerAdjustment))
      .flatMap(recordsThroughOffsets(_, timeout))

  def recordsThroughOffsetsOrZeros(
      finalOffsets: Map[TopicPartition, Long],
      pollTimeout: FiniteDuration,
      maxZeroCount: Int,
  )(implicit
      F: MonadError[F, Throwable]
  ): Stream[F, ConsumerRecords[K, V]] =
    // position is next offset consumer will read, assume already read up to offset before position
    Stream
      .eval(consumer.assignmentPositions.map(_.view.mapValues(_ - 1).toMap))
      .flatMap { initialOffsets =>
        consumer
          .recordsStream(pollTimeout)
          .mapAccumulate((initialOffsets, 0)) {
            case ((currentOffsets, zeroCount), records) =>
              (
                (
                  currentOffsets ++ records.lastOffsets,
                  if (records.count() == 0) zeroCount + 1 else 0,
                ),
                records,
              )
          }
          .takeThrough { case ((currentOffsets, zeroCount), _) =>
            finalOffsets.exists { case (tp, o) =>
              currentOffsets.get(tp).fold(true)(_ < o)
            } && zeroCount < maxZeroCount
          }
          .map(_._2)
        // once we've read up to final offset for a partition, we could pause it, so poll would stop returning any newer records from it, and then at end we could resume all partitions
        // not sure if pausing is really necessary, because this stream just needs to guarantee its read *up to* final offsets; reading *past* them should be fine
      }

  def recordsThroughAssignmentLastOffsetsOrZeros(
      pollTimeout: FiniteDuration,
      maxZeroCount: Int,
      commitMarkerAdjustment: Boolean = false,
  )(implicit F: MonadError[F, Throwable]): Stream[F, ConsumerRecords[K, V]] =
    Stream
      .eval(consumer.assignmentLastOffsets(commitMarkerAdjustment))
      .flatMap(recordsThroughOffsetsOrZeros(_, pollTimeout, maxZeroCount))

  /** Returns a sink that synchronously commits received offsets. */
  def commitSink: Pipe[F, Map[TopicPartition, OffsetAndMetadata], Unit] =
    _.evalMap(consumer.commitSync)

  /** Returns a stream that processes records using the specified function,
    * committing offsets for successfully processed records. If the processing
    * function returns a failure, the stream will halt with that failure, and
    * the record's offset will not be committed. This is still at-least-once
    * processing, but records for which the function returns success (and offset
    * commit succeeds) will not be reprocessed after a subsequent failure; only
    * records for which the function returns failure, or offset commits fail,
    * will be reprocessed. In some use cases this pattern is more appropriate
    * than just using auto-offset-commits, since it will not commit offsets for
    * failed records when the consumer is closed, and will likely result in less
    * reprocessing after a failure. The consumer must be configured to disable
    * offset auto-commits.
    */
  def readProcessCommit[A](pollTimeout: FiniteDuration)(
      process: ConsumerRecord[K, V] => F[A]
  )(implicit F: ApplicativeError[F, Throwable]): Stream[F, A] =
    consumer
      .recordStream(pollTimeout)
      .evalMap(r => process(r) <* consumer.commitSync(r.nextOffset))

  /** Returns a stream that processes records one-at-a-time using the specified
    * function, committing offsets for successfully processed records, either
    * after processing the specified number of records, or after the specified
    * time has elapsed since the last offset commit. On any stream finalization,
    * whether success, error, or cancelation, offsets will be committed. This is
    * at-least-once processing: after a restart, the record that failed will be
    * reprocessed. In some use cases this pattern is more appropriate than just
    * using auto-offset-commits, since it will not commit offsets for failed
    * records when the consumer is closed. The consumer must be configured to
    * disable offset auto-commits, i.e. `enable.auto.commit=false`.
    */
  def processingAndCommitting[A](
      pollTimeout: FiniteDuration,
      maxRecordCount: Long = 1000L,
      maxElapsedTime: FiniteDuration = 60.seconds,
      committedOffsetKeepAlive: Option[CommittedOffsetKeepAlive] = None,
  )(
      process: ConsumerRecord[K, V] => F[A]
  )(implicit S: Temporal[F]): Stream[F, A] =
    processingAndCommitting[ConsumerRecord[K, V], A](
      maxRecordCount,
      maxElapsedTime,
      committedOffsetKeepAlive,
    )(
      consumer.recordStream(pollTimeout),
      process,
      r => Map(new TopicPartition(r.topic, r.partition) -> r.offset),
      _ => 1,
    )

  /** Returns a stream that processes batches of records using the specified
    * function, committing offsets for successfully processed batches, either
    * after processing the specified number of records, or after the specified
    * time has elapsed since the last offset commit. On any stream finalization,
    * whether success, error, or cancelation, offsets will be committed. This is
    * at-least-once processing: after a restart, the batch that failed will be
    * reprocessed. In some use cases this pattern is more appropriate than just
    * using auto-offset-commits, since it will not commit offsets for failed
    * records when the consumer is closed. The consumer must be configured to
    * disable offset auto-commits, i.e. `enable.auto.commit=false`.
    */
  def processingAndCommittingBatched[A](
      pollTimeout: FiniteDuration,
      maxRecordCount: Long = 1000L,
      maxElapsedTime: FiniteDuration = 60.seconds,
  )(
      process: ConsumerRecords[K, V] => F[A]
  )(implicit S: Temporal[F]): Stream[F, A] =
    processingAndCommitting[ConsumerRecords[K, V], A](
      maxRecordCount,
      maxElapsedTime,
    )(
      consumer.recordsStream(pollTimeout),
      process,
      _.lastOffsets,
      _.count().toLong,
    )

  def processingAndCommitting[A, B](
      maxRecordCount: Long,
      maxElapsedTime: FiniteDuration,
  )(
      stream: Stream[F, A],
      process: A => F[B],
      offsets: A => Map[TopicPartition, Long],
      count: A => Long,
  )(implicit T: Temporal[F]): Stream[F, B] =
    processingAndCommitting(maxRecordCount, maxElapsedTime, None)(
      stream,
      process,
      offsets,
      count,
    )

  def processingAndCommitting[A, B](
      maxRecordCount: Long,
      maxElapsedTime: FiniteDuration,
      committedOffsetKeepAlive: Option[CommittedOffsetKeepAlive],
  )(
      stream: Stream[F, A],
      process: A => F[B],
      offsets: A => Map[TopicPartition, Long],
      count: A => Long,
  )(implicit T: Temporal[F]): Stream[F, B] = {
    val maxRecordCount0 =
      if (maxRecordCount > Int.MaxValue) Int.MaxValue
      else maxRecordCount.toInt

    val optKas: F[Option[(Semaphore[F], Ref[F, Map[TopicPartition, Long]])]] =
      committedOffsetKeepAlive.traverse {
        case CommittedOffsetKeepAlive(_, topics) =>
          for {
            lock <- Semaphore(1)
            offsets <- consumer.partitionQueries.committed(topics)
            ref <- Ref.of(
              offsets.view.mapValues(_.offset).toMap
            )
          } yield (lock, ref)
      }

    val init =
      (
        Queue.unbounded[F, Option[Map[TopicPartition, Long]]],
        optKas,
      ).tupled

    Stream.eval(init).flatMap { case (offsetQueue, optKas) =>
      def processAndEnqueueOffsets(a: A): F[B] = {
        val num = count(a).toInt
        val offsets0 = offsets(a)
        val commitInfo = Some(offsets0)

        for {
          b <- process(a).onError(_ =>
            offsetQueue.offer(None)
          ) // Signal to flush the commit stream
          _ <- offsetQueue
            .offer(commitInfo)
            .replicateA_(num) // Stuff the queue to trigger the barrier
        } yield b
      }

      def doCommit(chunk: Chunk[Map[TopicPartition, Long]]): F[Unit] =
        Temporal[F].uncancelable { _ =>
          (if (chunk.isEmpty) ().pure[F]
           else {
             val offsets =
               chunk.foldLeft(Map.empty[TopicPartition, Long])(_ ++ _)
             val nextOffsets =
               offsets.view.mapValues(o => new OffsetAndMetadata(o + 1)).toMap

             optKas match {
               case None => consumer.commitSync(nextOffsets)
               case Some((lock, ref)) =>
                 lock.permit.use { _ =>
                   consumer.commitSync(nextOffsets) *>
                   ref.update(_ ++ offsets)
                 }
             }
           })
        }

      def doKeepAlive(
          lock: Semaphore[F],
          ref: Ref[F, Map[TopicPartition, Long]],
      ): F[Unit] =
        lock.permit.use { _ =>
          for {
            offsets <- ref.get
            nextOffsets = offsets.view
              .mapValues(o => new OffsetAndMetadata(o + 1))
              .toMap
            _ <- consumer.commitSync(nextOffsets)
          } yield ()
        }

      val keepAliveStream: Stream[F, Unit] = {
        (committedOffsetKeepAlive.map(_.interval), optKas).tupled
          .map { case (interval, (lock, ref)) =>
            Stream.fixedRate(interval).evalMap(_ => doKeepAlive(lock, ref))
          }
          .getOrElse(Stream.empty)
      }

      val commitStream =
        Stream
          .fromQueueNoneTerminated(offsetQueue)
          .groupWithin(maxRecordCount0, maxElapsedTime)
          .evalMap(doCommit)

      val processStream =
        stream
          .evalMap(processAndEnqueueOffsets)
          .onFinalize(
            offsetQueue.offer(None)
          ) // Signal to flush the commit stream

      Stream.eval(commitStream.compile.drain.start) *>
      processStream
        .concurrently(keepAliveStream)
    }
  }
}

case class CommittedOffsetKeepAlive(
    interval: FiniteDuration,
    topics: Set[TopicPartition],
)
