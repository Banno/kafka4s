package com.banno.kafka

import cats.data.Kleisli
import org.apache.kafka.connect.connector.Task

package object connect {

  type StopConnector[F[_]] = F[Unit]

  type Connector[F[_], CC, TC, T <: Task] =
    Kleisli[F, (ConnectorContextApi[F], CC, Int), (List[TC], StopConnector[F])]

  type PollSourceFromOffset[F[_], P, O] = Kleisli[F, Map[P, O], List[SourceRecord[P, O]]]

  type StopTask[F[_]] = F[Unit]

  type InitializeSourceTask[F[_], P, O, TC <: TaskConfigs[P, O]] =
    Kleisli[F, SourceTaskContextApi[F, P, O, TC], (PollSourceFromOffset[F, P, O], StopTask[F])]
}
