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
