package com.banno.kafka.metrics.prometheus

import cats.implicits._
import cats.effect.IO
import com.banno.kafka._
import com.banno.kafka.producer._
import com.banno.kafka.consumer._
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import io.prometheus.client.CollectorRegistry
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters._
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

class PrometheusMetricsReporterApiSpec extends AnyFlatSpec with Matchers with InMemoryKafka {
  implicit val defaultContextShift = IO.contextShift(ExecutionContext.global)
  implicit val defaultConcurrent = IO.ioConcurrentEffect(defaultContextShift)
  implicit val defaultTimer = IO.timer(ExecutionContext.global)

  //when kafka clients change their metrics, this test will help identify the changes we need to make
  "Prometheus reporter" should "register Prometheus collectors for all known Kafka metrics and unregister on close" in {
    val topic = createTopic(2)
    val records =
      List(new ProducerRecord(topic, 0, "a", "a"), new ProducerRecord(topic, 1, "b", "b"))
    ProducerApi
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
                      } yield {
                        val registry = CollectorRegistry.defaultRegistry
                        registry.metricFamilySamples.asScala
                          .count(_.name.startsWith("kafka_producer")) should ===(56)
                        registry.metricFamilySamples.asScala
                          .find(_.name == "kafka_producer_record_send_total")
                          .map(_.samples.asScala.map(_.value)) should ===(Some(List(2)))

                        registry.metricFamilySamples.asScala
                          .count(_.name.startsWith("kafka_consumer")) should ===(50)
                        registry.metricFamilySamples.asScala
                          .find(_.name == "kafka_consumer_records_consumed_total")
                          .map(_.samples.asScala.map(_.value)) should ===(Some(List(2, 2)))
                        registry.metricFamilySamples.asScala
                          .find(_.name == "kafka_consumer_topic_records_consumed_total")
                          .map(_.samples.asScala.map(_.value)) should ===(Some(List(2, 2)))
                      }
                  )
            )
      )
      .unsafeRunSync()
      CollectorRegistry.defaultRegistry
        .metricFamilySamples.asScala
        .count(_.name.startsWith("kafka_producer")) should ===(0)
  }

}
