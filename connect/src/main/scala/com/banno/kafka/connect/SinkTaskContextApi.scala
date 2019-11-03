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
