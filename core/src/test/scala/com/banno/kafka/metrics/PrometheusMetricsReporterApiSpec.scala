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

import cats.syntax.all._
import cats.effect.IO
import com.banno.kafka._
import com.banno.kafka.producer._
import com.banno.kafka.consumer._
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import io.prometheus.client.CollectorRegistry
import munit._

import scala.jdk.CollectionConverters._
import scala.concurrent.duration._

class PrometheusMetricsReporterApiSpec extends CatsEffectSuite with DockerizedKafka {
  // This test allocates a lot of consumers. It was timing out in the GitHub
  // Actions build with the default amount of 30 seconds. That build seems to
  // have throttled resources compared to our development machines.
  override val munitTimeout = Duration(1, MINUTES)

  private def assertTotals(
    registry: CollectorRegistry,
    metric: String,
    expected: Option[List[Double]],
  ): Unit =
    assertEquals(
      registry.metricFamilySamples.asScala
        .find(_.name == metric)
        .map(
          _.samples
            .asScala
            .filter(_.name == s"${metric}_total")
            .map(_.value)
            .toList
        ),
      expected
    )

  // When Kafka clients change their metrics, this test will help identify the
  // changes we need to make
  test("Prometheus reporter should register Prometheus collectors for all known Kafka metrics and unregister on close") {
    for {
      topic <- createTopic[IO](2)
      records = List(
        new ProducerRecord(topic, 0, "a", "a"),
        new ProducerRecord(topic, 1, "b", "b")
      )
      () <- ProducerApi
        .resource[IO, String, String](
          BootstrapServers(bootstrapServer),
          MetricReporters[ProducerPrometheusReporter]
        )
        .use(
          p =>
            ConsumerApi
              .resource[IO, String, String](
                BootstrapServers(bootstrapServer),
                ClientId("c1"),
                MetricReporters[ConsumerPrometheusReporter]
              )
              .use(
                c1 =>
                  ConsumerApi
                    .resource[IO, String, String](
                      BootstrapServers(bootstrapServer),
                      ClientId("c2"),
                      MetricReporters[ConsumerPrometheusReporter]
                    )
                    .use(
                      c2 =>
                        for {
                          _ <- p.sendSyncBatch(records)

                          _ <- c1.assign(topic, Map.empty[TopicPartition, Long])
                          _ <- c1.poll(1 second)
                          _ <- c1.poll(1 second)

                          _ <- c2.assign(topic, Map.empty[TopicPartition, Long])
                          _ <- c2.poll(1 second)
                          _ <- c2.poll(1 second)

                          _ <- IO.sleep(PrometheusMetricsReporterApi.defaultUpdatePeriod + (1 second))

                          registry = CollectorRegistry.defaultRegistry
                          () = assertEquals(
                            registry.metricFamilySamples.asScala.count(_.name.startsWith("kafka_producer")),
                            56
                          )
                          () = assertTotals(
                            registry,
                            "kafka_producer_record_send",
                            List(2.0).some,
                          )

                          () = assertEquals(
                            registry.metricFamilySamples.asScala
                              .count(_.name.startsWith("kafka_consumer")),
                            50
                          )
                          () = assertTotals(
                            registry,
                            "kafka_consumer_records_consumed",
                            List(2.0, 2.0).some,
                          )
                          () = assertTotals(
                            registry,
                            "kafka_consumer_topic_records_consumed",
                            List(2.0, 2.0).some,
                          )
                        } yield ()
                    )
              )
        )
        finalMetricCount = CollectorRegistry.defaultRegistry
          .metricFamilySamples.asScala
          .count(_.name.startsWith("kafka_producer"))
        () = assertEquals(finalMetricCount, 0)
    } yield ()
  }
}
