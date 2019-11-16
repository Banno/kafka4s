package com.banno.kafka.connect

import cats.implicits._
import cats.data.Kleisli
import cats.effect.{Concurrent, IO}
import ConfigDefEncoder.Documentation

object example1 {

  object ExampleSourceConnectorApi {
    def apply[F[_]]: F[ConnectorApi[F]] = ???
  }

  class ExampleSourceConnector extends IOSourceConnector(ExampleSourceConnectorApi[IO])

}

object example2 {

  case class ExampleConfigs(
    @Documentation("todo")
    todo: String
  )

  object ExampleSourceConnectorApi {
    def apply[F[_]: Concurrent]: F[ConnectorApi[F]] = 
      ConnectorApi[F, ExampleConfigs, ExampleConfigs, ExampleSourceTask](
        "0.0.1", 
        Kleisli[F, (ConnectorContextApi[F], ExampleConfigs, Int), (List[ExampleConfigs], StopConnector[F])] {
          case (context, configs, maxTasks) => ???
        }.pure[F]
      )
  }

  class ExampleSourceTask extends IOSourceTask(???)

}

