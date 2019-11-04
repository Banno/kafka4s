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

import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.connect.connector.{ConnectorContext, Task}
import org.apache.kafka.common.config.Config
import cats.implicits._
import cats.effect.{Concurrent, Sync}
import cats.effect.concurrent.Deferred
import scala.reflect.ClassTag
import scala.reflect._
import scala.collection.JavaConverters._

trait ConnectorApi[F[_]] {
  def config: F[ConfigDef]
  def initialize(ctx: ConnectorContext): F[Unit]
  def initialize(ctx: ConnectorContext, taskConfigs: List[Map[String, String]]): F[Unit]
  def reconfigure(props: Map[String, String]): F[Unit]
  def start(props: Map[String, String]): F[Unit]
  def stop: F[Unit]
  def taskClass: F[Class[_ <: Task]]
  def taskConfigs(maxTasks: Int): F[List[Map[String, String]]]
  def validate(connectorConfigs: Map[String, String]): F[Config]
  def version: F[String]
}

object ConnectorApi {
  def apply[F[_]: Concurrent, CC: ConfigDefEncoder: MapDecoder, TC: MapEncoder, T <: Task: ClassTag](
      connectorVersion: String,
      connector: F[Connector[F, CC, TC, T]]
  ): F[ConnectorApi[F]] =
    for {
      context <- Deferred[F, ConnectorContextApi[F]]
      connectorConfigs <- Deferred[F, CC]
      stopConnector <- Deferred[F, F[Unit]]
      api = new ConnectorApi[F] {
        def config: F[ConfigDef] = ConfigDefEncoder[CC].encode.pure[F]
        def initialize(ctx: ConnectorContext): F[Unit] =
          context.complete(ConnectorContextApi[F](ctx))
        def initialize(ctx: ConnectorContext, taskConfigs: List[Map[String, String]]): F[Unit] =
          context.complete(ConnectorContextApi[F](ctx))
        def reconfigure(props: Map[String, String]): F[Unit] =
          ??? //TODO does Kafka Connect ever even call this? I can't find anywhere in KC code that this is called. Connector's impl of this calls stop and start...
        def start(props: Map[String, String]): F[Unit] =
          connectorConfigs.complete(MapDecoder[CC].decode(props))
        def stop: F[Unit] =
          stopConnector.get.flatten
        def taskClass: F[Class[_ <: Task]] =
          Sync[F].pure(classTag[T].runtimeClass.asInstanceOf[Class[_ <: Task]])
        def taskConfigs(maxTasks: Int): F[List[Map[String, String]]] =
          for {
            conn <- connector
            ctx <- context.get
            cc <- connectorConfigs.get
            (tcs, s) <- conn((ctx, cc, maxTasks))
            _ <- stopConnector.complete(s)
          } yield tcs.map(tc => MapEncoder[TC].encode(tc))
        def validate(props: Map[String, String]): F[Config] =
          for {
            cd <- config
            cvs <- Sync[F].delay(cd.validate(props.asJava))
            c <- Sync[F].delay(new Config(cvs))
            _ <- Sync[F].delay(MapDecoder[CC].decode(props)) //validate that we can parse the configs
          } yield c
        def version: F[String] = connectorVersion.pure[F]
      }
    } yield api
}
