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

import scala.concurrent.duration._

import cats._
import cats.data._
import cats.effect._
import cats.syntax.all._
import fs2.{Chunk, Stream}
import com.banno.kafka.consumer._
import com.banno.kafka.metrics.prometheus.ConsumerPrometheusReporter
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.clients.consumer._

sealed trait RecordStream[F[_], A] {
  def records: Stream[F, A]

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
    * reprocessing after a failure.
    */
  def readProcessCommit[B](process: A => F[B]): Stream[F, B]
}

sealed trait PastAndPresent[F[_], P[_[_], _], A] {
  def history: Stream[F, A]
  def present: P[F, A]

  protected final def catchUp(f: A => F[Unit])(implicit F: Async[F]): F[Unit] =
    history
      .evalMap(f)
      .compile
      .drain

  protected def presentStream: Stream[F, A]

  final def catchUpThenStream(
      f: A => F[Unit]
  )(implicit F: Async[F]): F[Stream[F, Unit]] =
    catchUp(f).map(_ => presentStream.evalMap(f))
}

sealed trait PastAndRecordStream[F[_], A] extends PastAndPresent[F, RecordStream, A] {
  def catchUpThenReadProcessCommit(
      f: A => F[Unit]
  ): F[Stream[F, Unit]]
}

object PastAndPresent {
  type Incoming[F[_], P[_[_], _], K, V] = PastAndPresent[F, P, IncomingRecord[K, V]]
  type Batched[F[_], P[_[_], _], A] = PastAndPresent[F, P, IncomingRecords[A]]
}

object PastAndStream {
  type T[F[_], A] = PastAndPresent[F, Stream, A]
  type Incoming[F[_], K, V] = T[F, IncomingRecord[K, V]]
  type Batched[F[_], A] = T[F, IncomingRecords[A]]

  private final case class Impl[F[_], A](
      history: Stream[F, A],
      present: Stream[F, A]
  ) extends PastAndPresent[F, Stream, A] {
    override protected def presentStream: Stream[F, A] = present
  }

  def apply[F[_], A](
      history: Stream[F, A],
      present: Stream[F, A]
  ): T[F, A] =
    Impl(history = history, present = present)

  object Batched {
    type Incoming[F[_], K, V] = Batched[F, IncomingRecord[K, V]]
  }
}

object PastAndRecordStream {
  type Incoming[F[_], K, V] = PastAndRecordStream[F, IncomingRecord[K, V]]
  type Batched[F[_], A] = PastAndRecordStream[F, IncomingRecords[A]]

  private final case class Impl[F[_]: Async, A](
      history: Stream[F, A],
      present: RecordStream[F, A]
  ) extends PastAndRecordStream[F, A] {
    override protected def presentStream: Stream[F, A] = present.records
    override def catchUpThenReadProcessCommit(
        f: A => F[Unit]
    ): F[Stream[F, Unit]] =
      catchUp(f).map(_ => present.readProcessCommit(f))
  }

  def apply[F[_]: Async, A](
      history: Stream[F, A],
      present: RecordStream[F, A]
  ): PastAndRecordStream[F, A] =
    Impl(history = history, present = present)

  object Batched {
    type Incoming[F[_], K, V] = Batched[F, IncomingRecord[K, V]]
  }
}

object RecordStream {
  type Incoming[F[_], K, V] = RecordStream[F, IncomingRecord[K, V]]
  type Batched[F[_], A] = RecordStream[F, IncomingRecords[A]]

  private abstract class Impl[F[_]: Applicative, A](
      val consumer: ConsumerApi[F, Array[Byte], Array[Byte]],
  ) extends RecordStream[F, A] {
    def readProcessCommit[B](process: A => F[B]): Stream[F, B] =
      records.evalMap { x =>
        process(x) <* consumer.commitSync(nextOffsets(x))
      }

    protected def nextOffsets(x: A): Map[TopicPartition, OffsetAndMetadata]
  }

  private def chunked[F[_], A](rs: IncomingRecords[A]): Stream[F, A] =
    Stream.chunk(Chunk.iterable(rs.toList))

  private sealed trait WhetherCommits[P[_[_], _]] {
    def extrude[F[_], A](x: RecordStream[F, A]): P[F, A]

    def chunk[F[_]: Applicative, A, B](
        topical: Topical[A, B],
    ): P[F, IncomingRecords[A]] => P[F, A]

    def pastAndPresent[F[_]: Async, A](
        history: Stream[F, A],
        present: P[F, A]
    ): PastAndPresent[F, P, A]

    def configs: List[(String, AnyRef)]
  }

  private object WhetherCommits {
    object No extends WhetherCommits[Stream] {
      override def extrude[F[_], A](x: RecordStream[F, A]): Stream[F, A] = x.records

      override def chunk[F[_]: Applicative, A, B](
          topical: Topical[A, B],
      ): Stream[F, IncomingRecords[A]] => Stream[F, A] =
        _.flatMap(chunked)

      override def pastAndPresent[F[_]: Async, A](
          history: Stream[F, A],
          present: Stream[F, A]
      ): PastAndPresent[F, Stream, A] = PastAndStream(history, present)

      override def configs: List[(String, AnyRef)] = List()
    }

    final case class May(groupId: GroupId) extends WhetherCommits[RecordStream] {
      override def extrude[F[_], A](x: RecordStream[F, A]): RecordStream[F, A] = x

      override def chunk[F[_]: Applicative, A, B](
          topical: Topical[A, B],
      ): RecordStream[F, IncomingRecords[A]] => RecordStream[F, A] =
        rs =>
          new Impl[F, A](
            rs match { case x: Impl[F, IncomingRecords[A]] => x.consumer }
          ) {
            override protected def nextOffsets(x: A) = topical.nextOffset(x)
            override def records: Stream[F, A] = rs.records.flatMap(chunked)
          }

      override def pastAndPresent[F[_]: Async, A](
          history: Stream[F, A],
          present: RecordStream[F, A]
      ): PastAndPresent[F, RecordStream, A] = PastAndRecordStream(history, present)

      override def configs: List[(String, AnyRef)] = List(groupId)
    }
  }

  sealed trait Subscriber[F[_], G[_]] {
    private[RecordStream] def whetherCommits: WhetherCommits[RecordStream]

    def to[A, B](
        topical: Topical[A, B],
        reset: AutoOffsetReset,
    ): Resource[F, RecordStream[F, G[A]]]
  }

  sealed trait Assigner[F[_], G[_], P[_[_], _]] {
    private[RecordStream] def whetherCommits: WhetherCommits[P]

    def presentWith[A, B](
        topical: Topical[A, B],
        reset: AutoOffsetReset,
        offsetsF: Kleisli[F, PartitionQueries[F], Map[TopicPartition, Long]],
    ): Resource[F, P[F, G[A]]]

    final def present[A, B](
        topical: Topical[A, B],
        reset: AutoOffsetReset,
        offsets: Map[TopicPartition, Long],
    )(implicit F: Applicative[F]): Resource[F, P[F, G[A]]] =
      presentWith(topical, reset, Kleisli.pure(offsets))

    final def present[A, B](
        topical: Topical[A, B]
    )(implicit F: Applicative[F]): Resource[F, P[F, G[A]]] =
      present(topical, AutoOffsetReset.earliest, Map.empty)

    def pastAndPresentWith[A, B](
        topical: Topical[A, B],
        offsetsF: Kleisli[F, PartitionQueries[F], Map[TopicPartition, Long]],
    ): Resource[F, PastAndPresent[F, P, G[A]]]

    // If you are interested in the past, then how does AutoOffsetReset.latest
    // make sense? Omitting the parameter until I see a clear reason for this.
    final def pastAndPresent[A, B](
        topical: Topical[A, B],
        offsets: Map[TopicPartition, Long],
    )(implicit F: Applicative[F]): Resource[F, PastAndPresent[F, P, G[A]]] =
      pastAndPresentWith(topical, Kleisli.pure(offsets))

    final def pastAndPresent[A, B](
        topical: Topical[A, B]
    )(implicit F: Applicative[F]): Resource[F, PastAndPresent[F, P, G[A]]] =
      pastAndPresent(topical, Map.empty)

    // If you are only interested in the past, then how does
    // AutoOffsetReset.latest make sense? Omitting the parameter until I see a
    // clear reason for this.
    def history[A, B](
        topical: Topical[A, B],
        offsets: Map[TopicPartition, Long],
    ): Resource[F, Stream[F, G[A]]]

    final def history[A, B](
        topical: Topical[A, B]
    ): Resource[F, Stream[F, G[A]]] =
      history(topical, Map.empty)
  }

  private final case class ChunkedSubscriber[F[_]: Async](
      val batched: Subscriber[F, IncomingRecords],
  ) extends Subscriber[F, Id] {
    override def whetherCommits = batched.whetherCommits

    override def to[A, B](
        topical: Topical[A, B],
        reset: AutoOffsetReset,
    ): Resource[F, RecordStream[F, A]] =
      batched
        .to(topical, reset)
        .map(whetherCommits.chunk(topical))
  }

  private final class ChunkedAssigner[F[_]: Async, P[_[_], _]](
      val batched: Assigner[F, IncomingRecords, P],
  ) extends Assigner[F, Id, P] {
    override def whetherCommits = batched.whetherCommits

    override def presentWith[A, B](
        topical: Topical[A, B],
        reset: AutoOffsetReset,
        offsetsF: Kleisli[F, PartitionQueries[F], Map[TopicPartition, Long]]
    ): Resource[F, P[F, A]] =
      batched
        .presentWith(topical, reset, offsetsF)
        .map(whetherCommits.chunk(topical))

    override def pastAndPresentWith[A, B](
        topical: Topical[A, B],
        offsetsF: Kleisli[F, PartitionQueries[F], Map[TopicPartition, Long]]
    ): Resource[F, PastAndPresent[F, P, A]] =
      batched
        .pastAndPresentWith(topical, offsetsF)
        .map { pp =>
          whetherCommits.pastAndPresent(
            history = pp.history.flatMap(chunked),
            present = whetherCommits.chunk[F, A, B](topical).apply(pp.present),
          )
        }

    def history[A, B](
        topical: Topical[A, B],
        offsets: Map[TopicPartition, Long],
    ): Resource[F, Stream[F, A]] =
      batched
        .history(topical, offsets)
        .map(_.flatMap(chunked))
  }

  private final case class BaseConfigs[P[_[_], _]](
      kafkaBootstrapServers: BootstrapServers,
      schemaRegistryUri: SchemaRegistryUrl,
      clientId: String,
      whetherCommits: WhetherCommits[P],
      // these extra configs are for testing confluent kafka auth https://banno-jha.atlassian.net/browse/KAYAK-708
      // they should eventually be replaced by proper types, after we figure out exactly what the configs should be
      extraConfigs: (String, AnyRef)*,
  ) {
    def consumerApi[F[_]: Async](
        reset: AutoOffsetReset
    ): Resource[F, ConsumerApi[F, Array[Byte], Array[Byte]]] = {
      val configs: List[(String, AnyRef)] =
        whetherCommits.configs ++
        extraConfigs ++
        List(
          kafkaBootstrapServers,
          schemaRegistryUri,
          EnableAutoCommit(false),
          reset,
          IsolationLevel.ReadCommitted,
          ClientId(clientId),
          MetricReporters[ConsumerPrometheusReporter],
        )
      ConsumerApi.ByteArray.resource[F](configs: _*)
    }
  }

  private object ChunkedAssigner {
    def apply[F[_]: Async, P[_[_], _]](
        kafkaBootstrapServers: BootstrapServers,
        schemaRegistryUri: SchemaRegistryUrl,
        clientId: String,
        whetherCommits: WhetherCommits[P],
    ): ChunkedAssigner[F, P] =
      new ChunkedAssigner(
        Batched.AssignerImpl(
          BaseConfigs(
            kafkaBootstrapServers,
            schemaRegistryUri,
            clientId,
            whetherCommits
          )
        )
      )
  }

  def subscribe[F[_]: Async](
      kafkaBootstrapServers: BootstrapServers,
      schemaRegistryUri: SchemaRegistryUrl,
      clientId: String,
      groupId: GroupId,
      extraConfigs: (String, AnyRef)*,
  ): Subscriber[F, Id] =
    new ChunkedSubscriber(
      Batched.SubscriberImpl(
        BaseConfigs(
          kafkaBootstrapServers,
          schemaRegistryUri,
          clientId,
          WhetherCommits.May(groupId),
          extraConfigs: _*,
        )
      )
    )

  def subscribe[F[_]: Async](
      kafkaBootstrapServers: BootstrapServers,
      schemaRegistryUri: SchemaRegistryUrl,
      groupId: GroupId,
      extraConfigs: (String, AnyRef)*,
  ): Subscriber[F, Id] =
    subscribe(
      kafkaBootstrapServers,
      schemaRegistryUri,
      groupId.id,
      groupId,
      extraConfigs: _*,
    )

  def assign[F[_]: Async](
      kafkaBootstrapServers: BootstrapServers,
      schemaRegistryUri: SchemaRegistryUrl,
      clientId: String,
  ): Assigner[F, Id, Stream] =
    ChunkedAssigner(
      kafkaBootstrapServers,
      schemaRegistryUri,
      clientId,
      WhetherCommits.No,
    )

  def assign[F[_]: Async](
      kafkaBootstrapServers: BootstrapServers,
      schemaRegistryUri: SchemaRegistryUrl,
      groupId: GroupId,
  ): Assigner[F, Id, RecordStream] =
    ChunkedAssigner(
      kafkaBootstrapServers,
      schemaRegistryUri,
      groupId.id,
      WhetherCommits.May(groupId),
    )

  def assign[F[_]: Async](
      kafkaBootstrapServers: BootstrapServers,
      schemaRegistryUri: SchemaRegistryUrl,
      clientId: String,
      groupId: GroupId,
  ): Assigner[F, Id, RecordStream] =
    ChunkedAssigner(
      kafkaBootstrapServers,
      schemaRegistryUri,
      clientId,
      WhetherCommits.May(groupId),
    )

  private def ephemeralTopicsSeekToEnd[F[_]: Monad](
      consumer: ConsumerApi[F, Array[Byte], Array[Byte]],
  ): AschematicTopic => F[Unit] =
    topic =>
      topic.purpose.contentType match {
        case TopicContentType.Ephemera =>
          consumer
            .partitionQueries
            .partitionsFor(topic.name.show)
            .flatMap(infos => consumer.seekToEnd(infos.map(_.toTopicPartition)))
        case _ => Applicative[F].unit
      }

  private val pollTimeout: FiniteDuration = 1.second

  object Batched {
    type Incoming[F[_], K, V] = Batched[F, IncomingRecord[K, V]]

    private def parseBatch[F[_]: ApplicativeThrow, A, B](
        topical: Topical[A, B]
    ): ConsumerRecords[Array[Byte], Array[Byte]] => F[IncomingRecords[A]] =
      IncomingRecords.parseWith(_, topical.parse).liftTo[F]

    private def recordStream[F[_]: Async, A, B](
        consumer: ConsumerApi[F, Array[Byte], Array[Byte]],
        topical: Topical[A, B],
    ): RecordStream[F, IncomingRecords[A]] =
      new Impl[F, IncomingRecords[A]](consumer) {
        override protected def nextOffsets(x: IncomingRecords[A]) =
          x.nextOffsets
        override def records: Stream[F, IncomingRecords[A]] =
          consumer
            .recordsStream(pollTimeout)
            .prefetch
            .evalMap(parseBatch(topical))
      }

    private[RecordStream] case class SubscriberImpl[F[_]: Async](
        baseConfigs: BaseConfigs[RecordStream]
    ) extends Subscriber[F, IncomingRecords] {
      override def whetherCommits = baseConfigs.whetherCommits

      override def to[A, B](
          topical: Topical[A, B],
          reset: AutoOffsetReset,
      ): Resource[F, RecordStream[F, IncomingRecords[A]]] =
        baseConfigs.consumerApi(reset).evalMap { consumer =>
          consumer
            .subscribe(topical.names.map(_.show).toList)
            .as(recordStream(consumer, topical))
        }
    }

    private[RecordStream] case class AssignerImpl[F[_]: Async, P[_[_], _]](
        baseConfigs: BaseConfigs[P]
    ) extends Assigner[F, IncomingRecords, P] {
      override def whetherCommits = baseConfigs.whetherCommits

      private def historyImpl[A, B](
          consumer: ConsumerApi[F, Array[Byte], Array[Byte]],
          topical: Topical[A, B],
      ): Stream[F, IncomingRecords[A]] =
        consumer
          .recordsThroughAssignmentLastOffsetsOrZeros(
            pollTimeout,
            10,
            commitMarkerAdjustment = true
          )
          .prefetch
          .evalMap(parseBatch(topical))

      private def presentImpl[A, B](
          consumer: ConsumerApi[F, Array[Byte], Array[Byte]],
          topical: Topical[A, B],
      ): P[F, IncomingRecords[A]] =
        whetherCommits.extrude(recordStream(consumer, topical))

      private def assign[A, B](
          consumer: ConsumerApi[F, Array[Byte], Array[Byte]],
          topical: Topical[A, B],
          offsetsF: Kleisli[F, PartitionQueries[F], Map[TopicPartition, Long]]
      ): F[Unit] =
        for {
          offsets <- offsetsF(consumer.partitionQueries)
          () <- consumer.assign(topical.names.map(_.show).toList, offsets)
          // By definition, no need to ever read old ephemera.
          () <- topical.aschematic.traverse_(ephemeralTopicsSeekToEnd(consumer))
        } yield ()

      override def presentWith[A, B](
          topical: Topical[A, B],
          reset: AutoOffsetReset,
          offsetsF: Kleisli[F, PartitionQueries[F], Map[TopicPartition, Long]]
      ): Resource[F, P[F, IncomingRecords[A]]] =
        baseConfigs.consumerApi(reset).evalMap { consumer =>
          assign(consumer, topical, offsetsF)
            .as(presentImpl(consumer, topical))
        }

      override def pastAndPresentWith[A, B](
          topical: Topical[A, B],
          offsetsF: Kleisli[F, PartitionQueries[F], Map[TopicPartition, Long]]
      ): Resource[F, PastAndPresent.Batched[F, P, A]] =
        baseConfigs.consumerApi(AutoOffsetReset.earliest).evalMap { consumer =>
          assign(consumer, topical, offsetsF)
            .as(
              whetherCommits.pastAndPresent(
                history = historyImpl(consumer, topical),
                present = presentImpl(consumer, topical),
              )
            )
        }

      override def history[A, B](
          topical: Topical[A, B],
          offsets: Map[TopicPartition, Long],
      ): Resource[F, Stream[F, IncomingRecords[A]]] =
        baseConfigs.consumerApi(AutoOffsetReset.earliest).evalMap { consumer =>
          assign(consumer, topical, Kleisli.pure(offsets))
            .as(historyImpl(consumer, topical))
        }
    }

    def subscribe[F[_]: Async](
        kafkaBootstrapServers: BootstrapServers,
        schemaRegistryUri: SchemaRegistryUrl,
        groupId: GroupId,
    ): Subscriber[F, IncomingRecords] =
      SubscriberImpl(
        BaseConfigs(
          kafkaBootstrapServers,
          schemaRegistryUri,
          groupId.id,
          WhetherCommits.May(groupId),
        )
      )

    def subscribe[F[_]: Async](
        kafkaBootstrapServers: BootstrapServers,
        schemaRegistryUri: SchemaRegistryUrl,
        clientId: String,
        groupId: GroupId,
    ): Subscriber[F, IncomingRecords] =
      SubscriberImpl(
        BaseConfigs(
          kafkaBootstrapServers,
          schemaRegistryUri,
          clientId,
          WhetherCommits.May(groupId),
        )
      )

    def assign[F[_]: Async](
        kafkaBootstrapServers: BootstrapServers,
        schemaRegistryUri: SchemaRegistryUrl,
        clientId: String,
    ): Assigner[F, IncomingRecords, Stream] =
      AssignerImpl(
        BaseConfigs(
          kafkaBootstrapServers,
          schemaRegistryUri,
          clientId,
          WhetherCommits.No,
        )
      )

    def assign[F[_]: Async](
        kafkaBootstrapServers: BootstrapServers,
        schemaRegistryUri: SchemaRegistryUrl,
        groupId: GroupId,
    ): Assigner[F, IncomingRecords, RecordStream] =
      AssignerImpl(
        BaseConfigs(
          kafkaBootstrapServers,
          schemaRegistryUri,
          groupId.id,
          WhetherCommits.May(groupId),
        )
      )

    def assign[F[_]: Async](
        kafkaBootstrapServers: BootstrapServers,
        schemaRegistryUri: SchemaRegistryUrl,
        clientId: String,
        groupId: GroupId,
    ): Assigner[F, IncomingRecords, RecordStream] =
      AssignerImpl(
        BaseConfigs(
          kafkaBootstrapServers,
          schemaRegistryUri,
          clientId,
          WhetherCommits.May(groupId),
        )
      )
  }
}
