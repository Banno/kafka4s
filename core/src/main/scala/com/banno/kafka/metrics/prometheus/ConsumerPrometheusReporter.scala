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

object ConsumerPrometheusReporter {
  // Chris Davenport: "You have walked into horrible territory. Like, the worst
  // territory I have ever seen." Given that, this is the right thing to do.
  // However, there is also a potentially different way to shim in this
  // impurity, but it would require a redesign.
  implicit val runtime: IORuntime = IORuntime.global

  /** The single instance used by all ConsumerPrometheusReporter instances. This
    * allows multiple Kafka consumers in the same JVM to each instantiate
    * ConsumerPrometheusReporter, while all still using the same Prometheus
    * collectors and registry properly. Metrics from multiple consumers are
    * distinguished by the `client_id` label. */
  val reporter: MetricsReporterApi[IO] =
    PrometheusMetricsReporterApi.consumer[IO]().unsafeRunSync()
}

/** Kafka consumer will instantiate this class via reflection.
  * Specify this consumer config: metric.reporters=com.banno.kafka.metrics.prometheus.ConsumerPrometheusReporter. */
class ConsumerPrometheusReporter extends IOMetricsReporter(ConsumerPrometheusReporter.reporter)
