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

package com.banno.kafka.metrics.epimetheus

import cats.syntax.all.*
import cats.effect.IO
import com.banno.kafka.*
import com.banno.kafka.producer.*
import com.banno.kafka.consumer.*
import natchez.Trace.Implicits.noop
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import io.prometheus.metrics.model.registry.PrometheusRegistry
import io.prometheus.metrics.model.snapshots.MetricSnapshots
import munit.*

import scala.concurrent.duration.*
import scala.jdk.OptionConverters.RichOptional

class EpimetheusMetricsReporterApiSpec
    extends CatsEffectSuite
    with DockerizedKafka {
  // This test allocates a lot of consumers. It was timing out in the GitHub
  // Actions build with the default amount of 30 seconds. That build seems to
  // have throttled resources compared to our development machines.
  override val munitIOTimeout = Duration(1, MINUTES)

  private def assertTotals(
      snapshots: MetricSnapshots,
      metric: String,
      expected: Option[List[Double]],
  ): Unit = {
    import scala.jdk.CollectionConverters._
    assertEquals(
      snapshots
        .stream()
        .filter(_.getMetadata.getName == metric)
        .findFirst()
        .toScala
        .map { snapshot =>
          snapshot.getDataPoints.asScala.toList.map { dp =>
            dp match {
              case counter: io.prometheus.metrics.model.snapshots.CounterSnapshot.CounterDataPointSnapshot =>
                counter.getValue
              case _ => 0.0
            }
          }
        },
      expected,
    )
  }

  // When Kafka clients change their metrics, this test will help identify the
  // changes we need to make
  test(
    "Epimetheus reporter should register Epimetheus collectors for all known Kafka metrics and unregister on close"
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
          MetricReporters[ProducerEpimetheusReporter],
        )
        .use(p =>
          ConsumerApi
            .resource[IO, String, String](
              BootstrapServers(bootstrapServer),
              ClientId("c1"),
              MetricReporters[ConsumerEpimetheusReporter],
            )
            .use(c1 =>
              ConsumerApi
                .resource[IO, String, String](
                  BootstrapServers(bootstrapServer),
                  ClientId("c2"),
                  MetricReporters[ConsumerEpimetheusReporter],
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
                        EpimetheusMetricsReporterApi.defaultUpdatePeriod + (1 second)
                      )

                    registry = PrometheusRegistry.defaultRegistry
                    snapshots = registry.scrape()
                    () = {
                      import scala.jdk.CollectionConverters._
                      import java.util.stream.Collectors
                      assertEquals(
                        snapshots
                          .stream()
                          .collect(Collectors.toList())
                          .asScala
                          .count(
                            _.getMetadata.getName.startsWith("kafka_producer")
                          ),
                        56,
                      )
                    }
                    () = assertTotals(
                      snapshots,
                      "kafka_producer_record_send",
                      List(2.0).some,
                    )

                    () = {
                      import scala.jdk.CollectionConverters._
                      import java.util.stream.Collectors
                      assertEquals(
                        snapshots
                          .stream()
                          .collect(Collectors.toList())
                          .asScala
                          .count(
                            _.getMetadata.getName.startsWith("kafka_consumer")
                          ),
                        50,
                      )
                    }
                    () = assertTotals(
                      snapshots,
                      "kafka_consumer_records_consumed",
                      List(2.0, 2.0).some,
                    )
                    () = assertTotals(
                      snapshots,
                      "kafka_consumer_topic_records_consumed",
                      List(2.0, 2.0).some,
                    )
                  } yield ()
                )
            )
        )
      finalSnapshots = PrometheusRegistry.defaultRegistry.scrape()
      finalMetricCount = {
        import scala.jdk.CollectionConverters._
        import java.util.stream.Collectors
        finalSnapshots
          .stream()
          .collect(Collectors.toList())
          .asScala
          .count(_.getMetadata.getName.startsWith("kafka_producer"))
      }
      () = assertEquals(finalMetricCount, 0)
    } yield ()
  }
}
