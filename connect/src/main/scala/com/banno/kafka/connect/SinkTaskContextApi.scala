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

import cats.effect.Sync
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.connect.sink.SinkTaskContext
import scala.concurrent.duration._
import scala.collection.JavaConverters._

trait SinkTaskContextApi[F[_], C] {
  def assignment: F[Set[TopicPartition]]
  def configs: F[C]
  def offsets(offsets: Map[TopicPartition, Long]): F[Unit]
  def offset(partition: TopicPartition, offset: Long): F[Unit]
  def pause(partitions: TopicPartition*): F[Unit]
  def requestCommit: F[Unit]
  def resume(partitions: TopicPartition*): F[Unit]
  def timeout(timeout: FiniteDuration): F[Unit]
}

object SinkTaskContextApi {
  def apply[F[_]: Sync, C: MapDecoder](ctx: SinkTaskContext): SinkTaskContextApi[F, C] =
    new SinkTaskContextApi[F, C] {
      override def assignment: F[Set[TopicPartition]] =
        Sync[F].delay(ctx.assignment().asScala.toSet)
      override def configs: F[C] = Sync[F].delay(MapDecoder[C].decode(ctx.configs.asScala.toMap))
      override def offsets(offsets: Map[TopicPartition, Long]): F[Unit] =
        Sync[F].delay(ctx.offset(offsets.mapValues(Long.box).asJava))
      override def offset(partition: TopicPartition, offset: Long): F[Unit] =
        Sync[F].delay(ctx.offset(partition, offset))
      override def pause(partitions: TopicPartition*): F[Unit] =
        Sync[F].delay(ctx.pause(partitions: _*))
      override def requestCommit: F[Unit] = Sync[F].delay(ctx.requestCommit())
      override def resume(partitions: TopicPartition*): F[Unit] =
        Sync[F].delay(ctx.resume(partitions: _*))
      override def timeout(timeout: FiniteDuration): F[Unit] =
        Sync[F].delay(ctx.timeout(timeout.toMillis))
    }
}
