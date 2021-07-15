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

import org.scalacheck._
import cats.effect._
import org.apache.kafka.clients.producer.ProducerRecord

import scala.concurrent.duration._
import com.banno.kafka.producer._
import com.banno.kafka.consumer._
import fs2._

import org.scalacheck.magnolia._
import com.sksamuel.avro4s.RecordFormat
import org.scalatestplus.scalacheck._

import org.scalatest.matchers.should.Matchers
import org.scalatest.propspec.AnyPropSpec

class ConsumerAndProducerApiSpec
    extends AnyPropSpec
    with ScalaCheckDrivenPropertyChecks
    with Matchers
    with DockerizedKafkaSpec {

  import cats.effect.unsafe.implicits.global

  private implicit def noShrink[T]: Shrink[T] = Shrink.shrinkAny

  private def producerRecord[K, V](topic: String)(p: (K, V)): ProducerRecord[K, V] =
    new ProducerRecord[K, V](topic, p._1, p._2)

  private def writeAndRead[F[_], K, V](
      producer: ProducerApi[F, K, V],
      consumer: ConsumerApi[F, K, V],
      topic: String,
      values: Vector[(K, V)]
  ): Stream[F, (K, V)] =
    Stream
      .emits(values)
      .covary[F]
      .map(producerRecord(topic))
      .evalMap(producer.sendAndForget)
      .drain ++
      Stream.eval(consumer.subscribe(topic)).drain ++
      consumer
        .recordStream(100 millis)
        .map(cr => (cr.key, cr.value))
        .take(values.size.toLong)

  implicit def stringRecordFormat: RecordFormat[String] =
    RecordFormat[String]
  implicit def personRecordFormat: RecordFormat[Person] =
    RecordFormat[Person]

  property("Avro serdes") {
    val groupId = genGroupId
    println(s"5 groupId=$groupId")
    val topic = createTopic()

    forAll { values: Vector[(String, Person)] =>
      val actual = (for {
        p <- Stream.resource(
          Avro4sProducer.resource[IO, String, Person](
            BootstrapServers(bootstrapServer),
            SchemaRegistryUrl(schemaRegistryUrl)
          )
        )
        c <- Stream.resource(
          Avro4sConsumer.resource[IO, String, Person](
            BootstrapServers(bootstrapServer),
            GroupId(groupId),
            AutoOffsetReset.earliest,
            SchemaRegistryUrl(schemaRegistryUrl)
          )
        )
        v <- writeAndRead(p, c, topic, values)
      } yield v).compile.toVector.unsafeRunSync()
      actual should ===(values)
    }
  }

  case class PersonId(id: String)
  case class Person2(name: String)
  implicit def personIdRecordFormat: RecordFormat[PersonId] =
    RecordFormat[PersonId]
  implicit def person2RecordFormat: RecordFormat[Person2] =
    RecordFormat[Person2]

  property("avro4s") {
    val groupId = genGroupId
    println(s"6 groupId=$groupId")
    val topic = createTopic()

    forAll { values: Vector[(PersonId, Person2)] =>
      val actual = (for {
        p <- Stream.resource(
          Avro4sProducer.resource[IO, PersonId, Person2](
            BootstrapServers(bootstrapServer),
            SchemaRegistryUrl(schemaRegistryUrl)
          )
        )
        c <- Stream.resource(
          Avro4sConsumer.resource[IO, PersonId, Person2](
            BootstrapServers(bootstrapServer),
            GroupId(groupId),
            AutoOffsetReset.earliest,
            SchemaRegistryUrl(schemaRegistryUrl)
          )
        )
        v <- writeAndRead(p, c, topic, values)
      } yield v).compile.toVector.unsafeRunSync()
      actual should ===(values)
    }
  }

}
