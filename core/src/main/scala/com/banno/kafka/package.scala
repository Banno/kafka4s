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

package com.banno

import cats.*
import cats.syntax.all.*
import fs2.Stream
import java.lang.{
  Double => JDouble,
  Float => JFloat,
  Integer => JInteger,
  Long => JLong,
  Short => JShort,
}
import java.nio.ByteBuffer
import java.util.{Map => JMap}
import org.apache.kafka.clients.consumer.{
  ConsumerRecord,
  ConsumerRecords,
  OffsetAndMetadata,
}
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.*
import org.apache.kafka.common.utils.Bytes
import org.apache.kafka.common.{PartitionInfo, TopicPartition}
import scala.jdk.CollectionConverters.*

package object kafka {

  implicit class ScalaProducerRecord[K, V](pr: ProducerRecord[K, V]) {
    def maybeKey: Option[K] = Option(pr.key)
    def maybeValue: Option[V] = Option(pr.value)
  }

  implicit class ScalaConsumerRecords[K, V](crs: ConsumerRecords[K, V]) {

    def recordList(topic: String): List[ConsumerRecord[K, V]] =
      crs.records(topic).asScala.toList

    def recordStream[F[_]](topic: String): Stream[F, ConsumerRecord[K, V]] =
      Stream.emits(recordList(topic)).covary[F]

    /** Returns the last (latest, highest) offset for each topic partition in
      * the collection of records.
      */
    // TODO this REALLY needs to be tested... assumes records for partition are in-order, calling .last hopefully never fails, etc
    def lastOffsets: Map[TopicPartition, Long] =
      crs.partitions.asScala.toSeq
        .map(tp => (tp -> crs.records(tp).asScala.last.offset))
        .toMap

    /** lastOffsets + 1, can be used to commit the offsets that the consumer
      * should read next, after these records.
      */
    def nextOffsets: Map[TopicPartition, OffsetAndMetadata] =
      lastOffsets.view.mapValues(o => new OffsetAndMetadata(o + 1)).toMap
  }

  implicit class ScalaConsumerRecord[K, V](cr: ConsumerRecord[K, V]) {
    def maybeKey: Option[K] = Option(cr.key)
    def maybeValue: Option[V] = Option(cr.value)

    def keyValue: (K, V) = (cr.key, cr.value)

    def offsetMap: Map[TopicPartition, OffsetAndMetadata] =
      Map(
        new TopicPartition(cr.topic, cr.partition) -> new OffsetAndMetadata(
          cr.offset
        )
      )
    def nextOffset: Map[TopicPartition, OffsetAndMetadata] =
      Map(
        new TopicPartition(cr.topic, cr.partition) -> new OffsetAndMetadata(
          cr.offset + 1
        )
      )
  }

  implicit class ByteArrayConsumerRecords(
      crs: ConsumerRecords[Array[Byte], Array[Byte]]
  ) {
    def recordListAs[K: Deserializer, V: Deserializer](
        topic: String
    ): List[ConsumerRecord[K, V]] =
      crs.recordList(topic).map(_.as[K, V](topic))

    def recordStreamAs[F[_], K: Deserializer, V: Deserializer](
        topic: String
    ): Stream[F, ConsumerRecord[K, V]] =
      crs.recordStream[F](topic).map(_.as[K, V](topic))
  }

  implicit def eqProducerRecord[K, V]: Eq[ProducerRecord[K, V]] =
    Eq.fromUniversalEquals // ProducerRecord implements equals properly

  implicit def eqConsumerRecord[K, V]: Eq[ConsumerRecord[K, V]] =
    new Eq[ConsumerRecord[K, V]] { // ConsumerRecord does not implement equals :(
      override def eqv(
          x: ConsumerRecord[K, V],
          y: ConsumerRecord[K, V],
      ): Boolean =
        x.topic == y.topic &&
        x.partition == y.partition &&
        x.offset == y.offset &&
        x.key == y.key && // might be better to use Eq[K] and Eq[V] but I couldn't align all the pieces to satisfy compiler
        x.value == y.value
    }

  implicit object ProducerRecordBitraverse extends Bitraverse[ProducerRecord] {
    override def bifoldLeft[A, B, C](fab: ProducerRecord[A, B], c: C)(
        f: (C, A) => C,
        g: (C, B) => C,
    ): C =
      g(f(c, fab.key), fab.value)

    override def bifoldRight[A, B, C](fab: ProducerRecord[A, B], c: Eval[C])(
        f: (A, Eval[C]) => Eval[C],
        g: (B, Eval[C]) => Eval[C],
    ): Eval[C] =
      g(fab.value, f(fab.key, c))

    /** Note that since a record's key or value could be null, functions f and g
      * should take care to handle null arguments. A null key means the sender
      * was unable to choose a key on which to partition. A null value means the
      * entity identified by the key should be deleted.
      */
    override def bitraverse[G[_]: Applicative, A, B, C, D](
        fab: ProducerRecord[A, B]
    )(f: A => G[C], g: B => G[D]): G[ProducerRecord[C, D]] =
      (f(fab.key), g(fab.value)).mapN(
        new ProducerRecord(
          fab.topic,
          fab.partition,
          fab.timestamp,
          _,
          _,
          fab.headers,
        )
      )
  }

  implicit object ConsumerRecordBitraverse extends Bitraverse[ConsumerRecord] {
    override def bifoldLeft[A, B, C](fab: ConsumerRecord[A, B], c: C)(
        f: (C, A) => C,
        g: (C, B) => C,
    ): C =
      g(f(c, fab.key), fab.value)

    override def bifoldRight[A, B, C](fab: ConsumerRecord[A, B], c: Eval[C])(
        f: (A, Eval[C]) => Eval[C],
        g: (B, Eval[C]) => Eval[C],
    ): Eval[C] =
      g(fab.value, f(fab.key, c))

    /** Note that since a record's key or value could be null, functions f and g
      * should take care to handle null arguments. A null key means the sender
      * was unable to choose a key on which to partition. A null value means the
      * entity identified by the key should be deleted. Note also that the
      * returned record has checksum of `-1` since that field is deprecated.
      */
    override def bitraverse[G[_]: Applicative, A, B, C, D](
        fab: ConsumerRecord[A, B]
    )(f: A => G[C], g: B => G[D]): G[ConsumerRecord[C, D]] =
      (f(fab.key), g(fab.value)).mapN(
        new ConsumerRecord(
          fab.topic,
          fab.partition,
          fab.offset,
          fab.timestamp,
          fab.timestampType,
          fab.serializedKeySize,
          fab.serializedValueSize,
          _,
          _,
          fab.headers,
          fab.leaderEpoch,
        )
      )
  }

  implicit class ByteArrayConsumerRecord(
      cr: ConsumerRecord[Array[Byte], Array[Byte]]
  ) {
    def maybeKeyAs[K](topic: String)(implicit kd: Deserializer[K]): Option[K] =
      cr.maybeKey.map(kd.deserialize(topic, _))
    def maybeValueAs[V](topic: String)(implicit
        vd: Deserializer[V]
    ): Option[V] =
      cr.maybeValue.map(vd.deserialize(topic, _))

    // note that these will probably throw NPE if key/value is null
    def keyAs[K](topic: String)(implicit kd: Deserializer[K]): K =
      kd.deserialize(topic, cr.key)
    def valueAs[V](topic: String)(implicit vd: Deserializer[V]): V =
      vd.deserialize(topic, cr.value)

    /** This only works when both key and value are non-null. */
    def as[K, V](topic: String)(implicit
        kd: Deserializer[K],
        vd: Deserializer[V],
    ): ConsumerRecord[K, V] =
      cr.bimap(kd.deserialize(topic, _), vd.deserialize(topic, _))
  }

  implicit object ConsumerRecordsBitraverse
      extends Bitraverse[ConsumerRecords] {
    override def bifoldLeft[A, B, C](fab: ConsumerRecords[A, B], c: C)(
        f: (C, A) => C,
        g: (C, B) => C,
    ): C =
      fab.partitions.asScala.toList
        .flatMap(fab.records(_).asScala.toList)
        .foldLeft(c)((b, a) => a.bifoldLeft(b)(f, g))

    override def bifoldRight[A, B, C](fab: ConsumerRecords[A, B], c: Eval[C])(
        f: (A, Eval[C]) => Eval[C],
        g: (B, Eval[C]) => Eval[C],
    ): Eval[C] =
      fab.partitions.asScala.toList
        .flatMap(fab.records(_).asScala.toList)
        .foldRight(c)((a, b) => a.bifoldRight(b)(f, g))

    override def bitraverse[G[_]: Applicative, A, B, C, D](
        fab: ConsumerRecords[A, B]
    )(f: A => G[C], g: B => G[D]): G[ConsumerRecords[C, D]] =
      fab.partitions.asScala.toList
        .traverse(tp =>
          fab
            .records(tp)
            .asScala
            .toList
            .traverse(_.bitraverse(f, g))
            .map(tp -> _.asJava)
        )
        .map(x => new ConsumerRecords[C, D](x.toMap.asJava))
  }

  implicit class BifunctorToOptionExtension[F[_, _], A, B](
      r: F[A, B]
  )(implicit F: Bifunctor[F]) {
    def toOption: F[Option[A], Option[B]] =
      F.bimap(r)(Option.apply, Option.apply)
  }

  implicit class RichPartitionInfo(pi: PartitionInfo) {
    def toTopicPartition: TopicPartition =
      new TopicPartition(pi.topic, pi.partition)
  }

  // when you create KafkaProducer with key/value Serializer instances, it will not call their configure methods
  // KafkaProducer only calls the Serializer's configure method when it uses the serializer class name in config to create the instance
  // same goes for KafkaConsumer/Deserializer
  implicit def byteArraySerializer: Serializer[Array[Byte]] =
    new ByteArraySerializer()
  implicit def byteBufferSerializer: Serializer[ByteBuffer] =
    new ByteBufferSerializer()
  implicit def bytesSerializer: Serializer[Bytes] = new BytesSerializer()
  implicit def jdoubleSerializer: Serializer[JDouble] = new DoubleSerializer()
  implicit def jfloatSerializer: Serializer[JFloat] = new FloatSerializer()
  implicit def jintegerSerializer: Serializer[JInteger] =
    new IntegerSerializer()
  implicit def jlongSerializer: Serializer[JLong] = new LongSerializer()
  implicit def jshortSerializer: Serializer[JShort] = new ShortSerializer()
  implicit def stringSerializer: Serializer[String] =
    new StringSerializer() // this will always use UTF8 encoding

  implicit def byteArrayDeserializer: Deserializer[Array[Byte]] =
    new ByteArrayDeserializer()
  implicit def byteBufferDeserializer: Deserializer[ByteBuffer] =
    new ByteBufferDeserializer()
  implicit def bytesDeserializer: Deserializer[Bytes] = new BytesDeserializer()
  implicit def jdoubleDeserializer: Deserializer[JDouble] =
    new DoubleDeserializer()
  implicit def jfloatDeserializer: Deserializer[JFloat] =
    new FloatDeserializer()
  implicit def jintegerDeserializer: Deserializer[JInteger] =
    new IntegerDeserializer()
  implicit def jlongDeserializer: Deserializer[JLong] = new LongDeserializer()
  implicit def jshortDeserializer: Deserializer[JShort] =
    new ShortDeserializer()
  implicit def stringDeserializer: Deserializer[String] =
    new StringDeserializer()

  // TODO test ContravariantLaws
  implicit object SerializerContravariant extends Contravariant[Serializer] {
    def contramap[A, B](fa: Serializer[A])(f: B => A): Serializer[B] =
      new Serializer[B] {
        override def close(): Unit = fa.close()
        override def configure(configs: JMap[String, ?], isKey: Boolean): Unit =
          fa.configure(configs, isKey)
        def serialize(topic: String, data: B): Array[Byte] =
          fa.serialize(topic, f(data))
      }
  }

  // TODO test FunctorLaws
  implicit object DeserializerFunctor extends Functor[Deserializer] {
    def map[A, B](fa: Deserializer[A])(f: A => B): Deserializer[B] =
      new Deserializer[B] {
        override def close(): Unit = fa.close()
        override def configure(configs: JMap[String, ?], isKey: Boolean): Unit =
          fa.configure(configs, isKey)
        def deserialize(topic: String, data: Array[Byte]): B = f(
          fa.deserialize(topic, data)
        )
      }
  }

  implicit def doubleSerializer: Serializer[Double] =
    Contravariant[Serializer].contramap(jdoubleSerializer)(Double.box)
  implicit def floatSerializer: Serializer[Float] =
    Contravariant[Serializer].contramap(jfloatSerializer)(Float.box)
  implicit def intSerializer: Serializer[Int] =
    Contravariant[Serializer].contramap(jintegerSerializer)(Int.box)
  implicit def longSerializer: Serializer[Long] =
    Contravariant[Serializer].contramap(jlongSerializer)(Long.box)
  implicit def shortSerializer: Serializer[Short] =
    Contravariant[Serializer].contramap(jshortSerializer)(Short.box)

  implicit def doubleDeserializer: Deserializer[Double] =
    Functor[Deserializer].map(jdoubleDeserializer)(Double.unbox)
  implicit def floatDeserializer: Deserializer[Float] =
    Functor[Deserializer].map(jfloatDeserializer)(Float.unbox)
  implicit def intDeserializer: Deserializer[Int] =
    Functor[Deserializer].map(jintegerDeserializer)(Int.unbox)
  implicit def longDeserializer: Deserializer[Long] =
    Functor[Deserializer].map(jlongDeserializer)(Long.unbox)
  implicit def shortDeserializer: Deserializer[Short] =
    Functor[Deserializer].map(jshortDeserializer)(Short.unbox)
}
