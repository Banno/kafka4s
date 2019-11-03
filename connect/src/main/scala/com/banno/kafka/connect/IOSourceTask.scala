package com.banno.kafka.connect

import org.apache.kafka.connect.source.{
  SourceRecord => KCSourceRecord,
  SourceTask,
  SourceTaskContext
}
import cats.effect.IO
import java.util.{List => JList, Map => JMap}
import scala.collection.JavaConverters._

abstract class IOSourceTask(apiIO: IO[SourceTaskApi[IO]]) extends SourceTask {
  val api: SourceTaskApi[IO] = apiIO.unsafeRunSync()
  override def commit(): Unit = 
    api.commit.unsafeRunSync()
  override def commitRecord(record: KCSourceRecord): Unit = 
    api.commitRecord(record).unsafeRunSync()
  override def initialize(context: SourceTaskContext): Unit =
    api.initialize(context).unsafeRunSync()
  override def poll(): JList[KCSourceRecord] = 
    api.poll.map(_.asJava).unsafeRunSync()
  override def start(props: JMap[String, String]): Unit =
    api.start(props.asScala.toMap).unsafeRunSync()
  override def stop(): Unit = 
    api.stop.unsafeRunSync()
  override def version(): String = 
    api.version.unsafeRunSync()
}
