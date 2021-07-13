package com.banno.kafka
package producer

import cats.effect._
import cats.syntax.all._
import org.apache.avro.generic.GenericRecord
import com.sksamuel.avro4s.ToRecord
import io.confluent.kafka.serializers.KafkaAvroSerializer

object Avro4sProducer {
  import ProducerApi._

  def apply[F[_], K, V](
    p: ProducerApi[F, GenericRecord, GenericRecord]
  )(implicit
      ktr: ToRecord[K],
    vtr: ToRecord[V],
  ): ProducerApi[F, K, V] =
    p.contrabimap(ktr.to, vtr.to)

  def resource[F[_]: Async, K: ToRecord, V: ToRecord](
      configs: (String, AnyRef)*
  ): Resource[F, ProducerApi[F, K, V]] =
    Resource.make(
      createKafkaProducer[F, GenericRecord, GenericRecord](
        (
          configs.toMap +
            KeySerializerClass(classOf[KafkaAvroSerializer]) +
            ValueSerializerClass(classOf[KafkaAvroSerializer])
        ).toSeq: _*
      ).map(p => Avro4sProducer[F, K, V](ProducerImpl.create(p)))
    )(_.close)
}
