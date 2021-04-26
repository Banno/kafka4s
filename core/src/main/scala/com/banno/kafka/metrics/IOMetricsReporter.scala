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

import org.apache.kafka.common.metrics.{KafkaMetric, MetricsReporter}
import cats.effect.IO
import cats.effect.unsafe.IORuntime
import java.util.{Map => JMap, List => JList}
import scala.jdk.CollectionConverters._

/** Adapts our pure MetricsReporterApi to Kafka's impure MetricsReporter. The
  * end of the universe for metrics reporters. Actual reporters should extend
  * this, so Kafka client can instantiate it via reflection. */
abstract class IOMetricsReporter(reporter: MetricsReporterApi[IO]) extends MetricsReporter {
  // TODO use Dispatcher instead?
  implicit val runtime: IORuntime = IORuntime.global

  override def configure(configs: JMap[String, _]): Unit =
    reporter.configure(configs.asScala.toMap).unsafeRunSync()

  override def init(metrics: JList[KafkaMetric]): Unit =
    reporter.init(metrics.asScala.toList).unsafeRunSync()

  override def metricChange(metric: KafkaMetric): Unit =
    reporter.add(metric).unsafeRunSync()

  override def metricRemoval(metric: KafkaMetric): Unit =
    reporter.remove(metric).unsafeRunSync()

  override def close(): Unit =
    reporter.close.unsafeRunSync()
}
