package com.banno.kafka.connect

import org.apache.kafka.connect.connector.Task
import cats.implicits._
import cats.data.Kleisli
import cats.effect.{Concurrent, Sync}
import scala.reflect.ClassTag
import scala.reflect._

object SingleTaskConnectorApi {
  def apply[F[_]: Concurrent, C: ConfigDefEncoder: MapEncoder: MapDecoder, T <: Task: ClassTag](
      connectorVersion: String
  ): F[ConnectorApi[F]] =
    ConnectorApi(
      connectorVersion,
      Kleisli[F, (ConnectorContextApi[F], C, Int), (List[C], StopConnector[F])] {
        case (_, configs, _) => (List(configs), Sync[F].unit).pure[F]
      }.pure[F]
    )
}
