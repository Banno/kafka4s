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

package com.banno.kafka.metrics

import scala.collection.compat._
import org.apache.kafka.common.metrics.KafkaMetric

/** So we can write pure code to report Kafka client metrics. */
trait MetricsReporterApi[F[_]] {

  /** Called once, with client's configs. */
  def configure(configs: Map[String, Any]): F[Unit]

  /** Called once, with all initial metrics. */
  def init(metrics: List[KafkaMetric]): F[Unit]

  /** Called when a new metric is registered. */
  def add(metric: KafkaMetric): F[Unit]

  /** Called when a metric is unregistered. */
  def remove(metric: KafkaMetric): F[Unit]

  /** Called once on shutdown. */
  def close: F[Unit]
}
