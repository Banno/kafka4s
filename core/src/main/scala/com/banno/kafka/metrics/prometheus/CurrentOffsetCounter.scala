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

package com.banno.kafka.metrics.prometheus

import cats.effect.Sync
import cats.implicits._
import org.apache.kafka.clients.consumer.ConsumerRecord
import io.prometheus.client._
import scala.math.max

object CurrentOffsetCounter {

  def apply[F[_]](cr: CollectorRegistry, prefix: String, clientId: String)(
      implicit F: Sync[F]): F[ConsumerRecord[_, _] => F[Unit]] =
    F.delay {
        Counter
          .build()
          .name(prefix + "_current_offset")
          .help("Counter for last consumed (not necessarily committed) offset of topic partition.")
          .labelNames("client_id", "topic", "partition")
          .register(cr)
      }
      .map { counter => (record: ConsumerRecord[_, _]) =>
        for {
          value <- F.delay(counter.labels(clientId, record.topic, record.partition.toString).get)
          delta = max(0, record.offset.toDouble - value)
          _ <- F.delay(counter.labels(clientId, record.topic, record.partition.toString).inc(delta))
        } yield ()
      }
}
