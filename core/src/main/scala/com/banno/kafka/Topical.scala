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
}
