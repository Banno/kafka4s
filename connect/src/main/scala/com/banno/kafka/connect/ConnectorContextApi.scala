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
import org.apache.kafka.connect.connector.ConnectorContext

trait ConnectorContextApi[F[_]] {
  def raiseError(e: Exception): F[Unit]
  def requestTaskReconfiguration: F[Unit]
}

object ConnectorContextApi {
  def apply[F[_]: Sync](ctx: ConnectorContext): ConnectorContextApi[F] =
    new ConnectorContextApi[F] {
      def raiseError(e: Exception): F[Unit] = Sync[F].delay(ctx.raiseError(e))
      def requestTaskReconfiguration: F[Unit] = Sync[F].delay(ctx.requestTaskReconfiguration())
    }
}
