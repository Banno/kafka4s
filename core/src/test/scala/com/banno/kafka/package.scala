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
