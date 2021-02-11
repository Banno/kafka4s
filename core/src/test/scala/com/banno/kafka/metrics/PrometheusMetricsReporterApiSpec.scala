package com.banno.kafka.metrics.prometheus

import scala.collection.compat._
import cats.syntax.all._
import cats.effect._
import com.banno.kafka._
import com.banno.kafka.producer._
import com.banno.kafka.consumer._
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import io.prometheus.client.CollectorRegistry
import munit.CatsEffectSuite
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters._
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import com.banno.kafka.test.{utils => TestUtils}

class PrometheusMetricsReporterApiSpec extends CatsEffectSuite {
  val resources =
    for {
      cp <- ConfluentContainers.resource[IO]
      bootstrapServer <- Resource.liftF(cp.bootstrapServers)
      topic <- Resource.liftF(TestUtils.createTopic[IO](bootstrapServer, 2))
      p <- ProducerApi.resource[IO, String, String](
        BootstrapServers(bootstrapServer),
        MetricReporters[ProducerPrometheusReporter],
      )
      c1 <- ConsumerApi.resource[IO, String, String](
        BootstrapServers(bootstrapServer),
        ClientId("c1"),
        MetricReporters[ConsumerPrometheusReporter],
      )
      c2 <- ConsumerApi.resource[IO, String, String](
        BootstrapServers(bootstrapServer),
        ClientId("c2"),
        MetricReporters[ConsumerPrometheusReporter],
      )
    } yield (topic, p, c1, c2)

  // TODO: Is there an additional test we can write to identify newly added metrics somehow?
  // This will identify any changes to existing metrics, but only those.
  ResourceFixture(resources)
    .test("Prometheus reporter registers collectors for known Kafka metrics") {
      case (topic, p, c1, c2) =>
        val records =
          List(new ProducerRecord(topic, 0, "a", "a"), new ProducerRecord(topic, 1, "b", "b"))
        val results = for {
          _ <- p.sendSyncBatch(records)

          _ <- c1.assign(topic, Map.empty[TopicPartition, Long])
          _ <- c1.poll(1.second)
          _ <- c1.poll(1.second)

          _ <- c2.assign(topic, Map.empty[TopicPartition, Long])
          _ <- c2.poll(1.second)
          _ <- c2.poll(1.second)

          _ <- IO.sleep(PrometheusMetricsReporterApi.defaultUpdatePeriod + (1.second))
          result = {
            val registry = CollectorRegistry.defaultRegistry
            List(
              registry.metricFamilySamples.asScala
                .count(_.name.startsWith("kafka_producer")) == (56),
              registry.metricFamilySamples.asScala
                .find(_.name == "kafka_producer_record_send_total")
                .map(_.samples.asScala.map(_.value)) == (Some(List(2))),
              registry.metricFamilySamples.asScala
                .count(_.name.startsWith("kafka_consumer")) == (50),
              registry.metricFamilySamples.asScala
                .find(_.name == "kafka_consumer_records_consumed_total")
                .map(_.samples.asScala.map(_.value)) == (Some(List(2, 2))),
              registry.metricFamilySamples.asScala
                .find(_.name == "kafka_consumer_topic_records_consumed_total")
                .map(_.samples.asScala.map(_.value)) == (Some(List(2, 2))),
            )
          }
        } yield result

        results.map(_.forall(identity)).assertEquals(true)
    }

}
