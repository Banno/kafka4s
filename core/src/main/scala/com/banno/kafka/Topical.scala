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

import scala.util._

import cats.data._
import cats.effect._

import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer._
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.clients.producer.ProducerRecord

trait AschematicTopic {
  def name: TopicName
  def purpose: TopicPurpose
}

/**
  * A Kafka topic or aggregation of topics.
  */
trait Topical[A, B] {
  def parse(cr: ConsumerRecord[GenericRecord, GenericRecord]): Try[A]

  def coparse(kv: B): ProducerRecord[GenericRecord, GenericRecord]

  def nextOffset(cr: A): Map[TopicPartition, OffsetAndMetadata]

  final def names: NonEmptyList[TopicName] =
    aschematic.map(_.name)

  def aschematic: NonEmptyList[AschematicTopic]

  def setUp[F[_]: Sync](
      bootstrapServers: BootstrapServers,
      schemaRegistryUri: SchemaRegistryUrl,
  ): F[Unit]

  def registerSchemas[F[_]: Sync](schemaRegistryUri: SchemaRegistryUrl): F[Unit]
}
