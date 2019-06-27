package com.banno.kafka.metrics.prometheus

import org.scalatest._
import cats.implicits._
import cats.effect.IO
import com.banno.kafka._
import com.banno.kafka.producer._
import com.banno.kafka.consumer._
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import io.prometheus.client.CollectorRegistry

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

class PrometheusMetricsReporterApiSpec extends FlatSpec with Matchers with InMemoryKafka {
  implicit val defaultContextShift = IO.contextShift(ExecutionContext.global)
  implicit val defaultConcurrent = IO.ioConcurrentEffect(defaultContextShift)
  implicit val defaultTimer = IO.timer(ExecutionContext.global)

  //when kafka clients change their metrics, this test will help identify the changes we need to make
  "Prometheus reporter" should "register Prometheus collectors for all known Kafka metrics" in {
    val topic = createTopic(2)
    val records =
      List(new ProducerRecord(topic, 0, "a", "a"), new ProducerRecord(topic, 1, "b", "b"))
    val io = for {
      p <- ProducerApi.create[IO, String, String](
        BootstrapServers(bootstrapServer),
        MetricReporters[ProducerPrometheusReporter]
      )
      _ <- p.sendSyncBatch(records)

      c1 <- ConsumerApi.create[IO, String, String](
        BootstrapServers(bootstrapServer),
        ClientId("c1"),
        MetricReporters[ConsumerPrometheusReporter]
      )
      _ <- c1.assign(topic, Map.empty[TopicPartition, Long])
      _ <- c1.poll(1 second)
      _ <- c1.poll(1 second)

      c2 <- ConsumerApi.create[IO, String, String](
        BootstrapServers(bootstrapServer),
        ClientId("c2"),
        MetricReporters[ConsumerPrometheusReporter]
      )
      _ <- c2.assign(topic, Map.empty[TopicPartition, Long])
      _ <- c2.poll(1 second)
      _ <- c2.poll(1 second)

      _ <- IO.sleep(PrometheusMetricsReporterApi.defaultUpdatePeriod + (1 second))
      _ <- p.close
      _ <- c1.close
      _ <- c2.close
    } yield {
      val registry = CollectorRegistry.defaultRegistry
      registry.metricFamilySamples.asScala.count(_.name.startsWith("kafka_producer")) should ===(56)
      registry.metricFamilySamples.asScala
        .find(_.name == "kafka_producer_record_send_total")
        .map(_.samples.asScala.map(_.value)) should ===(Some(List(2)))

      registry.metricFamilySamples.asScala.count(_.name.startsWith("kafka_consumer")) should ===(50)
      registry.metricFamilySamples.asScala
        .find(_.name == "kafka_consumer_records_consumed_total")
        .map(_.samples.asScala.map(_.value)) should ===(Some(List(2, 2)))
      registry.metricFamilySamples.asScala
        .find(_.name == "kafka_consumer_topic_records_consumed_total")
        .map(_.samples.asScala.map(_.value)) should ===(Some(List(2, 2)))
    }
    io.unsafeRunSync()
  }

}
