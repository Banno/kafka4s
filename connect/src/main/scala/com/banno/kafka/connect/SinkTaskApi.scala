package com.banno.kafka.connect

import org.apache.kafka.connect.sink.{SinkRecord, SinkTaskContext}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.clients.consumer.OffsetAndMetadata

trait SinkTaskApi[F[_]] extends TaskApi[F] {
  def close(partitions: Iterable[TopicPartition]): F[Unit]
  def flush(currentOffsets: Map[TopicPartition, OffsetAndMetadata]): F[Unit]
  def initialize(context: SinkTaskContext): F[Unit]
  def open(partitions: Iterable[TopicPartition]): F[Unit]
  def preCommit(
      currentOffsets: Map[TopicPartition, OffsetAndMetadata]
  ): F[Map[TopicPartition, OffsetAndMetadata]]
  def put(records: Iterable[SinkRecord]): F[Unit]
}
