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
package metrics.epimetheus

import cats.effect._
import cats.syntax.all._
import io.chrisdavenport.epimetheus._
import shapeless.Sized

object CurrentOffsetCounter {
  def apply[F[_]: Sync](
      cr: PrometheusRegistry[F],
      prefix: Name,
      clientId: String,
  ): F[IncomingRecordMetadata => F[Unit]] =
    Gauge
      .labelled(
        cr,
        prefix |+| Name("_current_offset"),
        "Gauge for last consumed (not necessarily committed) offset of topic partition.",
        Sized(
          Label("client_id"),
          Label("topic"),
          Label("partition"),
        ),
        (record: IncomingRecordMetadata) =>
          Sized(
            clientId,
            record.topicPartition.topic,
            record.topicPartition.partition.toString(),
          ),
      )
      .map { gauge => (record: IncomingRecordMetadata) =>
        gauge.label(record).set(record.offset.toDouble)
      }
}
