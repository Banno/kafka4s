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
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

class PrometheusMetricsReporterApiSpec extends CatsEffectSuite with DockerizedKafka {
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
                          () = assertEquals(
                            registry.metricFamilySamples.asScala
                              .find(_.name == "kafka_producer_record_send_total")
                              .map(_.samples.asScala.toList.map(_.value)),
                            List(2.0).some
                          )

                          () = assertEquals(
                            registry.metricFamilySamples.asScala
                              .count(_.name.startsWith("kafka_consumer")),
                            50
                          )
                          () = assertEquals(
                            registry.metricFamilySamples.asScala
                              .find(_.name == "kafka_consumer_records_consumed_total")
                              .map(_.samples.asScala.toList.map(_.value)),
                            List(2.0, 2.0).some
                          )
                          () = assertEquals(
                            registry.metricFamilySamples.asScala
                              .find(_.name == "kafka_consumer_topic_records_consumed_total")
                              .map(_.samples.asScala.toList.map(_.value)),
                            List(2.0, 2.0).some
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
