package com.banno.kafka
package consumer

import cats._
import cats.effect._
import cats.syntax.all._
import org.apache.avro.generic.GenericRecord
import com.sksamuel.avro4s.FromRecord
import io.confluent.kafka.serializers.KafkaAvroDeserializer

object Avro4sConsumer {
  import ConsumerApi._

  def apply[F[_]: Functor, K, V](
    c: ConsumerApi[F, GenericRecord, GenericRecord]
  )(implicit
      kfr: FromRecord[K],
    vfr: FromRecord[V],
  ): ConsumerApi[F, K, V] =
    c.bimap(kfr.from, vfr.from)

  def resource[F[_]: Async, K: FromRecord, V: FromRecord](
      configs: (String, AnyRef)*
  ): Resource[F, ConsumerApi[F, K, V]] =
    BlockingContext.resource.flatMap(
      e =>
        Resource.make(
          createKafkaConsumer[F, GenericRecord, GenericRecord](
            (
              configs.toMap +
                KeyDeserializerClass(classOf[KafkaAvroDeserializer]) +
                ValueDeserializerClass(classOf[KafkaAvroDeserializer])
            ).toSeq: _*
          ).map(c => Avro4sConsumer[F, K, V](ShiftingConsumer(ConsumerImpl(c), e)))
        )(_.close)
    )
}
