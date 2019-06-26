package com.banno.kafka

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

  //these things are necessary for EqSpec
  implicit def producerRecordCogen[K, V]: Cogen[ProducerRecord[K, V]] =
    Cogen(pr => pr.key.toString.length.toLong + pr.value.toString.length.toLong) // ¯\_(ツ)_/¯
  implicit def consumerRecordCogen[K, V]: Cogen[ConsumerRecord[K, V]] =
    Cogen(cr => cr.key.toString.length.toLong + cr.value.toString.length.toLong) // ¯\_(ツ)_/¯
}
