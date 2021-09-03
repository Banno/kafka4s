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
import org.apache.avro.generic.GenericRecord
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
      val consumer: ConsumerApi[F, GenericRecord, GenericRecord],
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

  private def toSeekTo(offsets: Map[TopicPartition, Long]): SeekTo =
    SeekTo.offsets(offsets, SeekTo.beginning)

  private def toSeekToF[F[_]: Functor](
      offsetsF: Kleisli[F, PartitionQueries[F], Map[TopicPartition, Long]]
  ): Kleisli[F, PartitionQueries[F], SeekTo] =
    offsetsF.map(toSeekTo)

  sealed trait Seeker[F[_], A] {
    protected implicit val F: Applicative[F]

    final def offsets(
        offsets: Map[TopicPartition, Long]
    ): A =
      seekTo(toSeekTo(offsets))

    final def seekTo(
        seekTo: SeekTo
    ): A =
      seekBy(Kleisli.pure(seekTo))

    final def offsetsBy(
        offsetsF: Kleisli[F, PartitionQueries[F], Map[TopicPartition, Long]]
    ): A =
      seekBy(toSeekToF(offsetsF))

    def seekBy(
        seekToF: Kleisli[F, PartitionQueries[F], SeekTo]
    ): A
  }

  private object Seeker {
    final case class Impl[F[_], A](
      apply: Kleisli[F, PartitionQueries[F], SeekTo] => A
    )(implicit val F: Applicative[F]) extends Seeker[F, A] {
      override def seekBy(
        seekToF: Kleisli[F, PartitionQueries[F], SeekTo]
      ): A = apply(seekToF)
    }

    implicit def applyInstance[F[_]]: Apply[Seeker[F, *]] =
      new Apply[Seeker[F, *]] {
        override def ap[A, B](
          ff: Seeker[F, A => B]
        )(
          fa: Seeker[F, A]
        ): Seeker[F, B] =
          ???

        override def map[A, B](fa: Seeker[F, A])(f: A => B): Seeker[F, B] =
          Impl { (seekToF: Kleisli[F, PartitionQueries[F], SeekTo]) =>
            f(fa.seekBy(seekToF))
          }(fa.F)
      }
  }

  sealed trait StreamSelector[F[_], G[_], P[_[_], _], A] {
    private[RecordStream] def whetherCommits: WhetherCommits[P]
    private[RecordStream] implicit val F: Async[F]
    private[RecordStream] implicit val G: Apply[G]

    def present: G[P[F, A]]
    def pastAndPresent: G[PastAndPresent[F, P, A]]
    def history: G[Stream[F, A]]

    final def mapK[H[_]: Apply](f: G ~> H): StreamSelector[F, H, P, A] =
      StreamSelector.Impl(
        f(history),
        f(present),
        whetherCommits,
      )
  }

  private object StreamSelector {
    final case class Impl[F[_], G[_], P[_[_], _], A](
      history: G[Stream[F, A]],
      present: G[P[F, A]],
      whetherCommits: WhetherCommits[P],
    )(implicit val F: Async[F], val G: Apply[G],
    ) extends StreamSelector[F, G, P, A] {
      override def pastAndPresent: G[PastAndPresent[F, P, A]] =
        history.product(present).map { pp =>
          whetherCommits.pastAndPresent(
            history = pp._1,
            present = pp._2,
          )
        }
    }
  }

  type SeekResource[F[_], A] =
    Seeker[F, Resource[F, A]]
  type SelectAndSeek[F[_], P[_[_], _], A] =
    StreamSelector[F, SeekResource[F, *], P, A]

  private def chunkedSelector[F[_]: Async, G[_], P[_[_], _], A, B](
    batched: StreamSelector[F, G, P, IncomingRecords[A]],
    topical: Topical[A, B],
  ): StreamSelector[F, G, P, A] = {
    implicit val ap: Apply[G] = batched.G
    StreamSelector.Impl(
      batched.history.map(_.flatMap(chunked)),
      batched.present.map(batched.whetherCommits.chunk(topical)),
      batched.whetherCommits,
    )
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

    override def history[A, B](
        topical: Topical[A, B],
        offsets: Map[TopicPartition, Long],
    ): Resource[F, Stream[F, A]] =
      batched
        .history(topical, offsets)
        .map(_.flatMap(chunked))
  }

  sealed trait ConfigStage1 {
    def client(clientId: String): ConfigStage2[Stream]
    def group(groupId: GroupId): ConfigStage2[RecordStream]
    def clientAndGroup(
        clientId: String,
        groupId: GroupId
    ): ConfigStage2[RecordStream]
  }

  sealed trait ConfigStage2[P[_[_], _]] {
    def untyped(configs: Seq[(String, AnyRef)]): ConfigStage2[P]

    def batched: ConfigStage3[P, IncomingRecords]
    def chunked: ConfigStage3[P, Id]
  }

  sealed trait ConfigStage3[P[_[_], _], G[_]] {
    def assign[F[_]: Async, A](
        topical: Topical[A, ?]
    ): SelectAndSeek[F, P, G[A]]
  }

  private final case class BaseConfigs[P[_[_], _]](
      kafkaBootstrapServers: BootstrapServers,
      schemaRegistryUri: SchemaRegistryUrl,
      clientId: String,
      whetherCommits: WhetherCommits[P],
      extraConfigs: Seq[(String, AnyRef)] = Seq.empty,
  ) extends ConfigStage2[P] {
    def consumerApiV2[F[_]: Async]: Resource[F, ConsumerApi[F, GenericRecord, GenericRecord]] = {
      val configs: List[(String, AnyRef)] =
        whetherCommits.configs ++
          extraConfigs ++
          List(
            kafkaBootstrapServers,
            schemaRegistryUri,
            EnableAutoCommit(false),
            IsolationLevel.ReadCommitted,
            ClientId(clientId),
            MetricReporters[ConsumerPrometheusReporter],
          )
      ConsumerApi.Avro.Generic.resource[F](configs: _*)
    }

    def consumerApi[F[_]: Async](
        reset: AutoOffsetReset
    ): Resource[F, ConsumerApi[F, GenericRecord, GenericRecord]] = {
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
      ConsumerApi.Avro.Generic.resource[F](configs: _*)
    }

    override def untyped(configs: Seq[(String, AnyRef)]) =
      copy(extraConfigs = configs)

    override val batched: ConfigStage3[P, IncomingRecords] = {
      val configs = this
      new ConfigStage3[P, IncomingRecords] {
        override def assign[F[_]: Async, A](
            topical: Topical[A, ?]
        ) = Batched.assignAndSeek(configs, topical)
      }
    }

    override val chunked: ConfigStage3[P, Id] =
      new ConfigStage3[P, Id] {
        override def assign[F[_]: Async, A](
            topical: Topical[A, ?]
        ) =
          chunkedSelector(
            batched.assign(topical),
            topical
          )
      }
  }

  def configure(
      kafkaBootstrapServers: BootstrapServers,
      schemaRegistryUri: SchemaRegistryUrl,
  ): ConfigStage1 =
    new ConfigStage1 {
      override def client(clientId: String): ConfigStage2[Stream] =
        BaseConfigs(
          kafkaBootstrapServers,
          schemaRegistryUri,
          clientId,
          WhetherCommits.No,
        )

      override def group(groupId: GroupId): ConfigStage2[RecordStream] =
        BaseConfigs(
          kafkaBootstrapServers,
          schemaRegistryUri,
          groupId.id,
          WhetherCommits.May(groupId),
        )

      override def clientAndGroup(
          clientId: String,
          groupId: GroupId
      ): ConfigStage2[RecordStream] =
        BaseConfigs(
          kafkaBootstrapServers,
          schemaRegistryUri,
          clientId,
          WhetherCommits.May(groupId),
        )
    }

  private object ChunkedAssigner {
    def apply[F[_]: Async, P[_[_], _]](
        kafkaBootstrapServers: BootstrapServers,
        schemaRegistryUri: SchemaRegistryUrl,
        clientId: String,
        whetherCommits: WhetherCommits[P],
        extraConfigs: Map[String, AnyRef],
    ): ChunkedAssigner[F, P] =
      new ChunkedAssigner(
        Batched.AssignerImpl(
          BaseConfigs(
            kafkaBootstrapServers,
            schemaRegistryUri,
            clientId,
            whetherCommits,
            extraConfigs.toList
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
          extraConfigs,
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
      Map.empty,
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
      Map.empty,
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
      Map.empty,
    )

  def assign[F[_]: Async](
      kafkaBootstrapServers: BootstrapServers,
      schemaRegistryUri: SchemaRegistryUrl,
      clientId: String,
      extraConfigs: Map[String, AnyRef],
  ): Assigner[F, Id, Stream] =
    ChunkedAssigner(
      kafkaBootstrapServers,
      schemaRegistryUri,
      clientId,
      WhetherCommits.No,
      extraConfigs,
    )

  def assign[F[_]: Async](
      kafkaBootstrapServers: BootstrapServers,
      schemaRegistryUri: SchemaRegistryUrl,
      groupId: GroupId,
      extraConfigs: Map[String, AnyRef],
  ): Assigner[F, Id, RecordStream] =
    ChunkedAssigner(
      kafkaBootstrapServers,
      schemaRegistryUri,
      groupId.id,
      WhetherCommits.May(groupId),
      extraConfigs,
    )

  def assign[F[_]: Async](
      kafkaBootstrapServers: BootstrapServers,
      schemaRegistryUri: SchemaRegistryUrl,
      clientId: String,
      groupId: GroupId,
      extraConfigs: Map[String, AnyRef],
  ): Assigner[F, Id, RecordStream] =
    ChunkedAssigner(
      kafkaBootstrapServers,
      schemaRegistryUri,
      clientId,
      WhetherCommits.May(groupId),
      extraConfigs,
    )

  private def ephemeralTopicsSeekToEnd[F[_]: Monad](
      consumer: ConsumerApi[F, GenericRecord, GenericRecord],
  ): AschematicTopic => F[Unit] =
    topic =>
      topic.purpose.contentType match {
        case TopicContentType.Ephemera =>
          consumer.partitionQueries
            .partitionsFor(topic.name.show)
            .flatMap(infos => consumer.seekToEnd(infos.map(_.toTopicPartition)))
        case _ => Applicative[F].unit
      }

  private val pollTimeout: FiniteDuration = 1.second

  object Batched {
    type Incoming[F[_], K, V] = Batched[F, IncomingRecord[K, V]]

    private def parseBatch[F[_]: ApplicativeThrow, A, B](
        topical: Topical[A, B]
    ): ConsumerRecords[GenericRecord, GenericRecord] => F[IncomingRecords[A]] =
      IncomingRecords.parseWith(_, topical.parse).liftTo[F]

    private def recordStream[F[_]: Async, A, B](
        consumer: ConsumerApi[F, GenericRecord, GenericRecord],
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

    private[RecordStream] type NeedsConsumer[F[_], A] =
      Function[ConsumerApi[F, GenericRecord, GenericRecord], A]

    private[RecordStream] def streamSelectorViaConsumer[F[_]: Async, P[_[_], _], A](
      whetherCommits: WhetherCommits[P],
      topical: Topical[A, ?],
    ): StreamSelector[F, NeedsConsumer[F, *], P, IncomingRecords[A]] = {
      val history: NeedsConsumer[F, Stream[F, IncomingRecords[A]]] =
        _.recordsThroughAssignmentLastOffsetsOrZeros(
          pollTimeout,
          10,
          commitMarkerAdjustment = true
        ).prefetch
          .evalMap(parseBatch(topical))
      val present: NeedsConsumer[F, P[F, IncomingRecords[A]]] =
        c => whetherCommits.extrude(recordStream(c, topical))
      StreamSelector.Impl(history, present, whetherCommits)
    }

    private def assign[F[_]: Monad, A, B](
        consumer: ConsumerApi[F, GenericRecord, GenericRecord],
        topical: Topical[A, B],
        seekToF: Kleisli[F, PartitionQueries[F], SeekTo],
    ): F[Unit] =
      for {
        seekTo <- seekToF(consumer.partitionQueries)
        () <- consumer.assignAndSeek(topical.names.map(_.show).toList, seekTo)
        // By definition, no need to ever read old ephemera.
        () <- topical.aschematic.traverse_(ephemeralTopicsSeekToEnd(consumer))
      } yield ()

    private[RecordStream] def assignAndSeek[F[_]: Async, P[_[_], _], X](
        baseConfigs: BaseConfigs[P],
        topical: Topical[X, ?],
    ): SelectAndSeek[F, P, IncomingRecords[X]] =
      streamSelectorViaConsumer(baseConfigs.whetherCommits, topical).mapK(
        new ~>[NeedsConsumer[F, *], SeekResource[F, *]] {
          override def apply[A](fa: NeedsConsumer[F, A]): SeekResource[F, A] =
            Seeker.Impl(
              (seekToF: Kleisli[F, PartitionQueries[F], SeekTo]) =>
                baseConfigs.consumerApiV2[F].evalMap { consumer =>
                  assign(consumer, topical, seekToF)
                  .as(fa(consumer))
                }
            )
        }
      )(Seeker.applyInstance.compose)

    private[RecordStream] case class AssignerImpl[F[_]: Async, P[_[_], _]](
        baseConfigs: BaseConfigs[P]
    ) extends Assigner[F, IncomingRecords, P] {
      override def whetherCommits = baseConfigs.whetherCommits

      private def historyImpl[A, B](
          consumer: ConsumerApi[F, GenericRecord, GenericRecord],
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
          consumer: ConsumerApi[F, GenericRecord, GenericRecord],
          topical: Topical[A, B],
      ): P[F, IncomingRecords[A]] =
        whetherCommits.extrude(recordStream(consumer, topical))

      private def assign[A, B](
          consumer: ConsumerApi[F, GenericRecord, GenericRecord],
          topical: Topical[A, B],
          seekToF: Kleisli[F, PartitionQueries[F], SeekTo]
      ): F[Unit] =
        for {
          seekTo <- seekToF(consumer.partitionQueries)
          () <- consumer.assignAndSeek(topical.names.map(_.show).toList, seekTo)
          // By definition, no need to ever read old ephemera.
          () <- topical.aschematic.traverse_(ephemeralTopicsSeekToEnd(consumer))
        } yield ()

      private def toSeekToF(
          offsetsF: Kleisli[F, PartitionQueries[F], Map[TopicPartition, Long]]
      ): Kleisli[F, PartitionQueries[F], SeekTo] =
        offsetsF.map(SeekTo.offsets(_, SeekTo.beginning))

      override def presentWith[A, B](
          topical: Topical[A, B],
          reset: AutoOffsetReset,
          offsetsF: Kleisli[F, PartitionQueries[F], Map[TopicPartition, Long]]
      ): Resource[F, P[F, IncomingRecords[A]]] =
        baseConfigs.consumerApi(reset).evalMap { consumer =>
          assign(consumer, topical, toSeekToF(offsetsF))
            .as(presentImpl(consumer, topical))
        }

      override def pastAndPresentWith[A, B](
          topical: Topical[A, B],
          offsetsF: Kleisli[F, PartitionQueries[F], Map[TopicPartition, Long]]
      ): Resource[F, PastAndPresent.Batched[F, P, A]] =
        baseConfigs.consumerApi(AutoOffsetReset.earliest).evalMap { consumer =>
          assign(consumer, topical, toSeekToF(offsetsF))
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
          assign(consumer, topical, toSeekToF(Kleisli.pure(offsets)))
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

    def assign[F[_]: Async](
        kafkaBootstrapServers: BootstrapServers,
        schemaRegistryUri: SchemaRegistryUrl,
        clientId: String,
        configs: Map[String, AnyRef],
    ): Assigner[F, IncomingRecords, Stream] =
      AssignerImpl(
        BaseConfigs(
          kafkaBootstrapServers,
          schemaRegistryUri,
          clientId,
          WhetherCommits.No,
          configs.toList
        )
      )

    def assign[F[_]: Async](
        kafkaBootstrapServers: BootstrapServers,
        schemaRegistryUri: SchemaRegistryUrl,
        groupId: GroupId,
        configs: Map[String, AnyRef],
    ): Assigner[F, IncomingRecords, RecordStream] =
      AssignerImpl(
        BaseConfigs(
          kafkaBootstrapServers,
          schemaRegistryUri,
          groupId.id,
          WhetherCommits.May(groupId),
          configs.toList
        )
      )

    def assign[F[_]: Async](
        kafkaBootstrapServers: BootstrapServers,
        schemaRegistryUri: SchemaRegistryUrl,
        clientId: String,
        groupId: GroupId,
        configs: Map[String, AnyRef],
    ): Assigner[F, IncomingRecords, RecordStream] =
      AssignerImpl(
        BaseConfigs(
          kafkaBootstrapServers,
          schemaRegistryUri,
          clientId,
          WhetherCommits.May(groupId),
          configs.toList
        )
      )
  }
}
