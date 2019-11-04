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
