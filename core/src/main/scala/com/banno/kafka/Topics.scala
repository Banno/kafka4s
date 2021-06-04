package com.banno.kafka

import scala.util._

import cats._
import cats.data._
import cats.effect._
import cats.syntax.all._
import com.sksamuel.avro4s._
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer._
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.clients.producer.ProducerRecord
import shapeless._

sealed trait Topics[A, B] extends Topical[A, B]

object Topics {
  final case class UnrecognizedTopic(record: Topic.CR) extends RuntimeException {
    override def getMessage(): String =
      s"Consumed a record from unrecognized topic ${record.topic()}"
  }

  private trait Impl[K, V, S <: Coproduct, T <: Coproduct]
      extends Topics[IncomingRecord[K, V] :+: S, (K, V) :+: T] {
    def topic: Topic[K, V]

    def tailParse(cr: ConsumerRecord[GenericRecord, GenericRecord]): Try[S]

    def tailCoparse(kv: T): ProducerRecord[GenericRecord, GenericRecord]

    def tailNextOffset(cr: S): Map[TopicPartition, OffsetAndMetadata]

    def tailSetUp[F[_]: Sync](
        bootstrapServers: BootstrapServers,
        schemaRegistryUri: SchemaRegistryUrl,
    ): F[Unit]

    final override def nextOffset(x: IncomingRecord[K, V] :+: S) =
      x.eliminate(topic.nextOffset, tailNextOffset)

    final override def parse(
        cr: ConsumerRecord[GenericRecord, GenericRecord]
    ): Try[IncomingRecord[K, V] :+: S] =
      if (cr.topic() === topic.name.show)
        /*then*/ topic.parse(cr).map(Inl.apply)
      else tailParse(cr).map(Inr.apply)

    final override def coparse(kv: (K, V) :+: T) =
      kv.eliminate(topic.coparse, tailCoparse)

    final override def setUp[F[_]: Sync](
        bootstrapServers: BootstrapServers,
        schemaRegistryUri: SchemaRegistryUrl,
    ): F[Unit] =
      topic.setUp(bootstrapServers, schemaRegistryUri) *>
      tailSetUp(bootstrapServers, schemaRegistryUri)
  }

  private final case class SingletonTopics[K, V](
      topic: Topic[K, V]
  ) extends Impl[K, V, CNil, CNil] {
    override def aschematic: NonEmptyList[AschematicTopic] = topic.aschematic
    override def tailParse(cr: Topic.CR): Try[CNil] = Failure(UnrecognizedTopic(cr))
    override def tailCoparse(kv: CNil) = kv.impossible
    override def tailNextOffset(x: CNil) = x.impossible
    override def tailSetUp[F[_]: Sync](
        bootstrapServers: BootstrapServers,
        schemaRegistryUri: SchemaRegistryUrl,
    ): F[Unit] = Applicative[F].unit
  }

  private final case class ConsTopics[K, V, S <: Coproduct, T <: Coproduct](
      topic: Topic[K, V],
      tail: Topics[S, T],
  ) extends Impl[K, V, S, T] {
    override def aschematic: NonEmptyList[AschematicTopic] =
      topic :: tail.aschematic

    override def tailNextOffset(x: S) = tail.nextOffset(x)

    override def tailParse(
        cr: ConsumerRecord[GenericRecord, GenericRecord]
    ): Try[S] = tail.parse(cr)

    override def tailCoparse(kv: T) = tail.coparse(kv)

    override def tailSetUp[F[_]: Sync](
        bootstrapServers: BootstrapServers,
        schemaRegistryUri: SchemaRegistryUrl,
    ): F[Unit] = tail.setUp(bootstrapServers, schemaRegistryUri)
  }

  def uncons[K, V, S <: Coproduct, T <: Coproduct](
      topics: Topics[IncomingRecord[K, V] :+: S, (K, V) :+: T]
  ): (Topic[K, V], Topics[S, T]) =
    topics match {
      case ConsTopics(topic, tail) => (topic, tail)
    }

  final case class Builder[A <: Coproduct, B <: Coproduct] private[Topics] (
      private val topics: Topics[A, B]
  ) {
    def and[K: FromRecord: ToRecord: SchemaFor, V: FromRecord: ToRecord: SchemaFor](
        topic: String,
        purpose: TopicPurpose
    ): Builder[IncomingRecord[K, V] :+: A, (K, V) :+: B] =
      and(Topic[K, V](topic, purpose))

    def and[K, V](
        topic: Topic[K, V]
    ): Builder[IncomingRecord[K, V] :+: A, (K, V) :+: B] =
      Builder(ConsTopics(topic, topics))

    def finis: Topics[A, B] = topics
  }

  def of[K, V](
      topic: Topic[K, V]
  ): Builder[IncomingRecord[K, V] :+: CNil, (K, V) :+: CNil] =
    Builder(SingletonTopics(topic))

  def of[K: FromRecord: ToRecord: SchemaFor, V: FromRecord: ToRecord: SchemaFor](
      topic: String,
      purpose: TopicPurpose
  ): Builder[IncomingRecord[K, V] :+: CNil, (K, V) :+: CNil] =
    of(Topic[K, V](topic, purpose))
}
