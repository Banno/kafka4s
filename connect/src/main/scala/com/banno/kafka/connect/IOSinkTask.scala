package com.banno.kafka.connect

import org.apache.kafka.connect.sink.{SinkRecord, SinkTask, SinkTaskContext}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import cats.effect.IO
import java.util.{Map => JMap, Collection => JCollection}
import scala.collection.JavaConverters._

abstract class IOSinkTask(apiIO: IO[SinkTaskApi[IO]]) extends SinkTask {
  val api: SinkTaskApi[IO] = apiIO.unsafeRunSync()
  override def close(partitions: JCollection[TopicPartition]): Unit =
    api.close(partitions.asScala).unsafeRunSync()
  override def flush(currentOffsets: JMap[TopicPartition, OffsetAndMetadata]): Unit =
    api.flush(currentOffsets.asScala.toMap).unsafeRunSync()
  override def initialize(context: SinkTaskContext): Unit = 
    api.initialize(context).unsafeRunSync()
  override def open(partitions: JCollection[TopicPartition]): Unit =
    api.open(partitions.asScala).unsafeRunSync()
  override def preCommit(
      currentOffsets: JMap[TopicPartition, OffsetAndMetadata]
  ): JMap[TopicPartition, OffsetAndMetadata] =
    api.preCommit(currentOffsets.asScala.toMap).map(_.asJava).unsafeRunSync()
  override def put(records: JCollection[SinkRecord]): Unit =
    api.put(records.asScala).unsafeRunSync()
  override def start(props: JMap[String, String]): Unit =
    api.start(props.asScala.toMap).unsafeRunSync()
  override def stop(): Unit = 
    api.stop.unsafeRunSync()
  override def version(): String = 
    api.version.unsafeRunSync()
}
