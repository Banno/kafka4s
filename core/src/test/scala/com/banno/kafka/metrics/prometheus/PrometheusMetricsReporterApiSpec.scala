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

import cats.syntax.all.*
import cats.effect.IO
import com.banno.kafka.*
import com.banno.kafka.producer.*
import com.banno.kafka.consumer.*
import natchez.Trace.Implicits.noop
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import io.prometheus.metrics.model.registry.PrometheusRegistry as JPrometheusRegistry
import munit.*

import scala.jdk.CollectionConverters.*
import io.prometheus.metrics.model.snapshots.CounterSnapshot.CounterDataPointSnapshot
import io.prometheus.metrics.model.snapshots.GaugeSnapshot.GaugeDataPointSnapshot
import io.prometheus.metrics.model.snapshots.MetricSnapshots

import scala.concurrent.duration.*

class PrometheusMetricsReporterApiSpec
    extends CatsEffectSuite
    with DockerizedKafka {
  // This test allocates a lot of consumers. It was timing out in the GitHub
  // Actions build with the default amount of 30 seconds. That build seems to
  // have throttled resources compared to our development machines.
  override val munitIOTimeout = Duration(1, MINUTES)

  private def assertTotals(
      sample: MetricSnapshots,
      metric: String,
      expected: Option[List[Double]],
  ): Unit =
    assertEquals(
      sample
        .iterator()
        .asScala
        .toList
        .find(_.getMetadata.getName == metric)
        .map(
          _.getDataPoints.asScala.toList.map {
            case counter: CounterDataPointSnapshot => counter.getValue
            case gauge: GaugeDataPointSnapshot => gauge.getValue
            case e => throw new Exception(s"Got snapshot ${e}")
          }
        ),
      expected,
    )

  // When Kafka clients change their metrics, this test will help identify the
  // changes we need to make
  test(
    "Prometheus reporter should register collectors for all known Kafka metrics and unregister on close"
  ) {
    for {
      topic <- createTopic[IO](2)
      records = List(
        new ProducerRecord(topic, 0, "a", "a"),
        new ProducerRecord(topic, 1, "b", "b"),
      )
      () <- ProducerApi
        .resource[IO, String, String](
          BootstrapServers(bootstrapServer),
          MetricReporters[ProducerPrometheusReporter],
        )
        .use(p =>
          ConsumerApi
            .resource[IO, String, String](
              BootstrapServers(bootstrapServer),
              ClientId("c1"),
              MetricReporters[ConsumerPrometheusReporter],
            )
            .use(c1 =>
              ConsumerApi
                .resource[IO, String, String](
                  BootstrapServers(bootstrapServer),
                  ClientId("c2"),
                  MetricReporters[ConsumerPrometheusReporter],
                )
                .use(c2 =>
                  for {
                    _ <- p.sendAsyncBatch(records)

                    _ <- c1.assign(topic, Map.empty[TopicPartition, Long])
                    _ <- c1.poll(1 second)
                    _ <- c1.poll(1 second)

                    _ <- c2.assign(topic, Map.empty[TopicPartition, Long])
                    _ <- c2.poll(1 second)
                    _ <- c2.poll(1 second)

                    _ <- IO
                      .sleep(
                        PrometheusMetricsReporterApi.defaultUpdatePeriod + (1 second)
                      )

                    metrics = JPrometheusRegistry.defaultRegistry.scrape()

                    () = assertEquals(
                      metrics.asScala.toList.count(
                        _.getMetadata.getName.startsWith("kafka_producer")
                      ),
                      53,
                    )
                    () = assertTotals(
                      metrics,
                      "kafka_producer_record_send",
                      List(2.0).some,
                    )

                    () = assertEquals(
                      metrics.asScala.toList.count(
                        _.getMetadata.getName.startsWith("kafka_consumer")
                      ),
                      48,
                    )
                    () = assertTotals(
                      metrics,
                      "kafka_consumer_records_consumed",
                      List(2.0, 2.0).some,
                    )
                    () = assertTotals(
                      metrics,
                      "kafka_consumer_topic_records_consumed",
                      List(2.0, 2.0).some,
                    )
                  } yield ()
                )
            )
        )
      finalMetricCount =
        JPrometheusRegistry.defaultRegistry
          .scrape()
          .asScala
          .toList
          .count(_.getMetadata.getName.startsWith("kafka_producer"))
      () = assertEquals(finalMetricCount, 0)
    } yield ()
  }
}
