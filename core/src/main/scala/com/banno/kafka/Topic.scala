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

import java.time.Instant

import scala.jdk.CollectionConverters.*
import scala.util.*
import scala.util.control.NoStackTrace

import cats.*
import cats.data.*
import cats.effect.*
import cats.syntax.all.*
import com.banno.kafka.admin.AdminApi
import com.banno.kafka.schemaregistry.SchemaRegistryApi
import com.sksamuel.avro4s.FromRecord
import com.sksamuel.avro4s.SchemaFor
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.consumer._
import org.apache.kafka.clients.producer.ProducerRecord
import com.sksamuel.avro4s.ToRecord

trait Topic[K, V]
    extends Topical[IncomingRecord[K, V], (K, V)]
    with AschematicTopic {
  final override def aschematic: NonEmptyList[AschematicTopic] =
    NonEmptyList.one(this)

  def withParseFailedHandler(
    handle: Topic.ParseFailed => Try[IncomingRecord[K, V]]
  ): Topic[K, V]
}

object Topic {
  type CR = ConsumerRecord[GenericRecord, GenericRecord]

  private def fromGeneric[A](
      gr: GenericRecord
  )(implicit FR: FromRecord[A]): Try[A] =
    Try(FR.from(gr))

  sealed trait KeyOrValue {
    def forLog: String
    def select(cr: CR): GenericRecord
    override def toString(): String = forLog
  }

  object Key extends KeyOrValue {
    override def forLog: String = "key"
    override def select(cr: CR): GenericRecord = cr.key()
  }

  object Value extends KeyOrValue {
    override def forLog: String = "value"
    override def select(cr: CR): GenericRecord = cr.value()
  }

  private def parse1[K: FromRecord, V: FromRecord](cr: CR): Try[(K, V)] = {
    val tryKey = fromGeneric[K](cr.key).adaptError(ParseFailed(Key, cr, _))
    val tryValue =
      fromGeneric[V](cr.value).adaptError(ParseFailed(Value, cr, _))
    tryKey.product(tryValue)
  }

  final case class ParseFailed(
      keyOrValue: KeyOrValue,
      cr: CR,
      cause: Throwable,
  ) extends RuntimeException(cause)
      with NoStackTrace {
    override def getMessage(): String = {
      val loc = s"${cr.topic()}/${cr.partition()}/${cr.offset()}"
      val time = Instant.ofEpochMilli(cr.timestamp())
      val raw = keyOrValue.select(cr)
      s"Unable to parse record $keyOrValue from $loc @[$time]:\n$raw"
    }
  }

  private final case class Impl[
      K: FromRecord: ToRecord: SchemaFor,
      V: FromRecord: ToRecord: SchemaFor,
  ](
      topic: String,
      topicPurpose: TopicPurpose,
      handlePF: Topic.ParseFailed => Try[IncomingRecord[K, V]],
  ) extends Topic[K, V] {
    override def coparse(
        kv: (K, V)
    ): ProducerRecord[GenericRecord, GenericRecord] =
      new ProducerRecord(topic, kv._1, kv._2).toGenericRecord

    override def name: TopicName = TopicName(topic)

    override def purpose: TopicPurpose = topicPurpose

    override def parse(cr: CR): Try[IncomingRecord[K, V]] =
      parse1[K, V](cr).map { kv =>
        IncomingRecord.of(cr).bimap(_ => kv._1, _ => kv._2)
      }

    override def withParseFailedHandler(
      handle: Topic.ParseFailed => Try[IncomingRecord[K, V]]
    ): Topic[K, V] =
      copy(handlePF = handle)

    override def handleParseFailed(failed: Topic.ParseFailed): Try[IncomingRecord[K, V]] =
      handlePF(failed)

    override def nextOffset(x: IncomingRecord[K, V]) =
      x.metadata.nextOffset

    private def config: NewTopic =
      new NewTopic(
        topic,
        purpose.partitions,
        purpose.replicationFactor,
        // The .configs(...) method performs mutation, but as long as we keep it
        // local to the method that has done the `new`, the result is referentially
        // transparent.
      ).configs(purpose.configs.toMap.asJava)

    override def registerSchemas[F[_]: Sync](
        schemaRegistryUri: SchemaRegistryUrl,
        configs: Map[String, Object] = Map.empty,
    ): F[Unit] =
      SchemaRegistryApi
        .register[F, K, V](
          schemaRegistryUri.url,
          topic,
          configs,
        )
        .void

    override def setUp[F[_]: Sync](
        bootstrapServers: BootstrapServers,
        schemaRegistryUri: SchemaRegistryUrl,
        configs: Map[String, Object] = Map.empty,
    ): F[Unit] =
      for {
        _ <- AdminApi.createTopicsIdempotent(
          bootstrapServers.bs,
          List(config),
          configs,
        )
        _ <- registerSchemas(schemaRegistryUri, configs)
      } yield ()
  }

  def apply[
      K: FromRecord: ToRecord: SchemaFor,
      V: FromRecord: ToRecord: SchemaFor,
  ](
      topic: String,
      topicPurpose: TopicPurpose,
  ): Topic[K, V] =
    Impl(topic, topicPurpose, Failure.apply)

  private final case class InvariantImpl[K, A, B](
    fa: Topic[K, A],
    f: A => B,
    g: B => A,
    handlePF: Option[Topic.ParseFailed => Try[IncomingRecord[K, B]]],
  ) extends Topic[K, B] {
    override def parse(
        cr: ConsumerRecord[GenericRecord, GenericRecord]
    ): Try[IncomingRecord[K, B]] =
      fa.parse(cr).map(_.map(f))


    override def handleParseFailed(failed: Topic.ParseFailed): Try[IncomingRecord[K, B]] =
      handlePF.fold(
        fa.handleParseFailed(failed).map(_.map(f))
      )(h => h(failed))

    override def withParseFailedHandler(
      handle: Topic.ParseFailed => Try[IncomingRecord[K, B]]
    ): Topic[K, B] =
      copy(handlePF = handle.some)

    override def coparse(
        kv: (K, B)
    ): ProducerRecord[GenericRecord, GenericRecord] =
      fa.coparse((kv._1, g(kv._2)))

    override def nextOffset(
        cr: IncomingRecord[K, B]
    ) = cr.metadata.nextOffset

    override def registerSchemas[F[_]: Sync](
        schemaRegistryUri: SchemaRegistryUrl,
        configs: Map[String, Object] = Map.empty,
    ): F[Unit] = fa.registerSchemas(schemaRegistryUri, configs)

    override def setUp[F[_]: Sync](
        bootstrapServers: BootstrapServers,
        schemaRegistryUri: SchemaRegistryUrl,
        configs: Map[String, Object] = Map.empty,
    ): F[Unit] = fa.setUp(bootstrapServers, schemaRegistryUri, configs)

    override def name: TopicName = fa.name
    override def purpose: TopicPurpose = fa.purpose
  }

  implicit def invariant[K]: Invariant[Topic[K, *]] =
    new Invariant[Topic[K, *]] {
      override def imap[A, B](
          fa: Topic[K, A]
      )(f: A => B)(g: B => A): Topic[K, B] =
        InvariantImpl(fa, f, g, None)
    }
}
