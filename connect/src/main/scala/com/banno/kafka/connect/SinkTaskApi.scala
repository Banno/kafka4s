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
