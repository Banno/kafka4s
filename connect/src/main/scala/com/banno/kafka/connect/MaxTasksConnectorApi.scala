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

import org.apache.kafka.connect.connector.Task
import cats.implicits._
import cats.data.Kleisli
import cats.effect.{Concurrent, Sync}
import scala.reflect.ClassTag
import scala.reflect._

object MaxTasksConnectorApi {
  def apply[F[_]: Concurrent, C: ConfigDefEncoder: MapEncoder: MapDecoder, T <: Task: ClassTag](
      connectorVersion: String
  ): F[ConnectorApi[F]] =
    ConnectorApi(
      connectorVersion,
      Kleisli[F, (ConnectorContextApi[F], C, Int), (List[C], StopConnector[F])] {
        case (_, configs, maxTasks) => (List.fill(maxTasks)(configs), Sync[F].unit).pure[F]
      }.pure[F]
    )
}
