package com.banno.kafka

import cats.effect._
import cats.syntax.all._
import com.banno.kafka.admin.AdminApi
import org.apache.kafka.clients.admin.NewTopic
import org.scalacheck.{Arbitrary, Cogen, Gen}
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.consumer.ConsumerRecord

package object test {

  implicit def arbitraryProducerRecord[K: Arbitrary, V: Arbitrary]
      : Arbitrary[ProducerRecord[K, V]] = Arbitrary {
    for {
      t <- Gen.identifier
      k <- Arbitrary.arbitrary[K]
      v <- Arbitrary.arbitrary[V]
    } yield new ProducerRecord(t, k, v)
  }

  implicit def arbitraryConsumerRecord[K: Arbitrary, V: Arbitrary]
      : Arbitrary[ConsumerRecord[K, V]] = Arbitrary {
    for {
      t <- Gen.identifier
      p <- Gen.posNum[Int]
      o <- Gen.posNum[Long]
      k <- Arbitrary.arbitrary[K]
      v <- Arbitrary.arbitrary[V]
    } yield new ConsumerRecord(t, p, o, k, v)
  }

  implicit def producerRecordCogen[K, V]: Cogen[ProducerRecord[K, V]] =
    Cogen(pr => pr.key.toString.length.toLong + pr.value.toString.length.toLong)
  implicit def consumerRecordCogen[K, V]: Cogen[ConsumerRecord[K, V]] =
    Cogen(cr => cr.key.toString.length.toLong + cr.value.toString.length.toLong)

  object utils {
    def randomId(): String = Gen.listOfN(10, Gen.alphaChar).map(_.mkString).sample.get
    def genGroupId(): String = randomId()
    def genTopic(): String = randomId()

    def createTopic[F[_]: Sync](bootstrapServer: String, partitionCount: Int = 1): F[String] =
      for {
        topic <- Sync[F].delay(genTopic())
        _ <- AdminApi
          .createTopicsIdempotent[F](
            bootstrapServer,
            List(new NewTopic(topic, partitionCount, 1.toShort))
          )
      } yield topic
  }
}
