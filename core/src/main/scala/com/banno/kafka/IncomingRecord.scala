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

import cats._
import cats.syntax.all._
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.clients.consumer._
import scala.jdk.CollectionConverters._
import shapeless._
import org.apache.avro.generic.GenericRecord

final case class TopicPartitionOffset(
    topic: String,
    partition: Int,
    offset: Long,
) {
  override def toString(): String =
    s"$topic-$partition@$offset"

  def toMap: Map[TopicPartition, OffsetAndMetadata] =
    Map(new TopicPartition(topic, partition) -> new OffsetAndMetadata(offset))
}

sealed trait IncomingRecordMetadata {
  def timestamp: Long
  def topicPartitionOffset: TopicPartitionOffset
  def nextOffset: Map[TopicPartition, OffsetAndMetadata]
  def headers: Map[String, Array[Byte]]

  final def topicPartition: TopicPartition =
    new TopicPartition(topicPartitionOffset.topic, topicPartitionOffset.partition)
  final def offset: Long = topicPartitionOffset.offset
  final def topic: String = topicPartitionOffset.topic
  final def partition: Int = topicPartitionOffset.partition
}

object IncomingRecordMetadata {
  trait Of[A] {
    def apply(x: A): IncomingRecordMetadata
  }

  implicit val ofCNil: Of[CNil] = new Of[CNil] {
    override def apply(x: CNil): IncomingRecordMetadata = x.impossible
  }

  implicit def ofRecord[K, V]: Of[IncomingRecord[K, V]] =
    new Of[IncomingRecord[K, V]] {
      override def apply(x: IncomingRecord[K, V]): IncomingRecordMetadata =
        x.metadata
    }

  implicit def ofCons[H: Of, T <: Coproduct: Of]: Of[H :+: T] =
    new Of[H :+: T] {
      override def apply(x: H :+: T): IncomingRecordMetadata =
        x.eliminate(of(_), of(_))
    }

  def of[A](x: A)(implicit Of: Of[A]): IncomingRecordMetadata = Of(x)

  object Batched {
    def of[A](xs: List[A])(implicit Of: Of[A]): List[IncomingRecordMetadata] =
      xs.map(Of(_))
  }
}

// By extending `Product2`, we allow ourselves elsewhere to write code that
// processes these records without even having a direct dependency on this
// library: in pure Scala you can write handler code as long as you only care
// about the key and value.
//
// Implemented not as a final case class but as a trait because we anticipate
// adding more metadata fields to it later.
sealed trait IncomingRecord[K, V] extends Product2[K, V] {
  def key: K
  def value: V
  final override def _1: K = key
  final override def _2: V = value
  def metadata: IncomingRecordMetadata
}

object IncomingRecord {
  private final case class Metadata(
      timestamp: Long,
      topicPartitionOffset: TopicPartitionOffset,
      nextOffset: Map[TopicPartition, OffsetAndMetadata],
      headers: Map[String, Array[Byte]],
  ) extends IncomingRecordMetadata

  private final case class Impl[K, V](
      key: K,
      value: V,
      metadata: Metadata
  ) extends IncomingRecord[K, V]

  // Exposing this as the only constructor allows us to add more fields that are
  // on the consumer record to the incoming record in a backwards-compatible
  // manner.
  def of[K, V](cr: ConsumerRecord[K, V]): IncomingRecord[K, V] =
    Impl(
      cr.key(),
      cr.value(),
      Metadata(
        cr.timestamp(),
        TopicPartitionOffset(cr.topic(), cr.partition(), cr.offset()),
        cr.nextOffset,
        cr.headers.asScala.map(h => (h.key, h.value)).toMap,
      )
    )

  implicit val bitraverse: Bitraverse[IncomingRecord] =
    new Bitraverse[IncomingRecord] {
      override def bifoldLeft[A, B, C](
          fab: IncomingRecord[A, B],
          c: C
      )(f: (C, A) => C, g: (C, B) => C): C =
        g(f(c, fab.key), fab.value)

      // TODO I'm not super familiar with Bifoldable. Hopefully these are
      // lawful. If we really cared we could write tests.
      override def bifoldRight[A, B, C](
          fab: IncomingRecord[A, B],
          c: Eval[C]
      )(f: (A, Eval[C]) => Eval[C], g: (B, Eval[C]) => Eval[C]): Eval[C] =
        f(fab.key, g(fab.value, c))

      override def bitraverse[G[_]: Applicative, A, B, C, D](
          fab: IncomingRecord[A, B]
      )(f: A => G[C], g: B => G[D]): G[IncomingRecord[C, D]] =
        (f(fab.key), g(fab.value)).mapN { (k, v) =>
          Impl(k, v, fab.metadata match { case x: Metadata => x })
        }
    }

  // Expose a right-biased functor instance
  implicit def functor[K]: Functor[IncomingRecord[K, *]] =
    bitraverse.rightFunctor

  // Expose a right-biased traverse instance
  implicit def traverse[K]: Traverse[IncomingRecord[K, *]] =
    new Traverse[IncomingRecord[K, *]] {
      def foldLeft[A, B](
          fa: IncomingRecord[K, A],
          b: B
      )(f: (B, A) => B): B =
        f(b, fa.value)

      def foldRight[A, B](
          fa: IncomingRecord[K, A],
          lb: Eval[B]
      )(f: (A, Eval[B]) => Eval[B]): Eval[B] =
        f(fa.value, lb)

      def traverse[G[_]: Applicative, A, B](
          fa: IncomingRecord[K, A]
      )(f: A => G[B]): G[IncomingRecord[K, B]] =
        fa.bitraverse(_.pure[G], f)
    }
}

sealed trait IncomingRecords[A] {
  def toList: List[A]
  def nextOffsets: Map[TopicPartition, OffsetAndMetadata]
}

object IncomingRecords {
  type T[K, V] = IncomingRecords[IncomingRecord[K, V]]

  private final case class Impl[A](
      toList: List[A],
      nextOffsets: Map[TopicPartition, OffsetAndMetadata],
  ) extends IncomingRecords[A]

  def parseWith[F[_]: ApplicativeThrow, A](
      cr: ConsumerRecords[GenericRecord, GenericRecord],
      f: ConsumerRecord[GenericRecord, GenericRecord] => F[A]
  ): F[IncomingRecords[A]] =
    cr.asScala.toList
      .traverse(f)
      .map(
        Impl(_, cr.nextOffsets)
      )
}
