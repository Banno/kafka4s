package com.banno.kafka.connect

import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.connect.connector.{ConnectorContext, Task, Connector => KCConnector}
import org.apache.kafka.common.config.Config
import scala.collection.JavaConverters._
import cats.effect.IO
import java.util.{List => JList, Map => JMap}
import scala.collection.JavaConverters._
import org.apache.kafka.connect.source.SourceConnector
import org.apache.kafka.connect.sink.SinkConnector

trait IOConnector extends KCConnector {
  def api: ConnectorApi[IO]
  override def config(): ConfigDef =
    api.config.unsafeRunSync()
  override def initialize(ctx: ConnectorContext): Unit =
    api.initialize(ctx).unsafeRunSync()
  override def initialize(ctx: ConnectorContext, taskConfigs: JList[JMap[String, String]]): Unit =
    api.initialize(ctx, taskConfigs.asScala.toList.map(_.asScala.toMap)).unsafeRunSync()
  override def reconfigure(props: JMap[String, String]): Unit =
    api.reconfigure(props.asScala.toMap).unsafeRunSync()
  override def start(props: JMap[String, String]): Unit =
    api.start(props.asScala.toMap).unsafeRunSync()
  override def stop(): Unit =
    api.stop.unsafeRunSync()
  override def taskClass(): Class[_ <: Task] =
    api.taskClass.unsafeRunSync() //TODO can this be a type parameter?
  override def taskConfigs(maxTasks: Int): JList[JMap[String, String]] =
    api.taskConfigs(maxTasks).map(_.map(_.asJava).asJava).unsafeRunSync()
  override def validate(connectorConfigs: JMap[String, String]): Config =
    api.validate(connectorConfigs.asScala.toMap).unsafeRunSync()
  override def version(): String =
    api.version.unsafeRunSync()
}

abstract class IOSourceConnector(apiIO: IO[ConnectorApi[IO]])
    extends SourceConnector
    with IOConnector {
  override val api: ConnectorApi[IO] = apiIO.unsafeRunSync()
}

abstract class IOSinkConnector(apiIO: IO[ConnectorApi[IO]]) extends SinkConnector with IOConnector {
  override val api: ConnectorApi[IO] = apiIO.unsafeRunSync()
}
