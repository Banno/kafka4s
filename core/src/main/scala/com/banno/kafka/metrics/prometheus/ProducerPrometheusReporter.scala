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

import com.banno.kafka.metrics._
import cats.effect.IO
import cats.effect.unsafe.IORuntime

object ProducerPrometheusReporter {
  // TODO use Dispatcher instead?
  implicit val runtime: IORuntime = IORuntime.global

  /** The single instance used by all ProducerPrometheusReporter instances. This
    * allows multiple Kafka producers in the same JVM to each instantiate
    * ProducerPrometheusReporter, while all still using the same Prometheus
    * collectors and registry properly. Metrics from multiple producers are
    * distinguished by the `client_id` label. */
  val reporter: MetricsReporterApi[IO] =
    PrometheusMetricsReporterApi.producer[IO]().unsafeRunSync()
}

/** Kafka producer will instantiate this class via reflection.
  * Specify this producer config:
  * metric.reporters=com.banno.kafka.metrics.prometheus.ProducerPrometheusReporter. */
class ProducerPrometheusReporter extends IOMetricsReporter(ProducerPrometheusReporter.reporter)
