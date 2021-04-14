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

import cats._
import cats.data._
import cats.effect.concurrent.Ref
import cats.effect._
import cats.syntax.all._
import com.banno.kafka.metrics.MetricsReporterApi
import fs2.Stream
import fs2.concurrent.SignallingRef
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import io.prometheus.client._
import org.apache.kafka.common.MetricName
import org.apache.kafka.common.metrics.KafkaMetric
import scala.collection.compat._
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._
import scala.math.max

object PrometheusMetricsReporterApi {

  def log[G[_]: Sync] = Slf4jLogger.getLoggerFromClass(this.getClass)

  def underscore(s: String): String = s.replaceAll("""\W""", "_")

  case class MetricId(group: String, name: String, tags: List[String])
  object MetricId {
    def apply(m: KafkaMetric): MetricId =
      MetricId(
        m.metricName.group,
        m.metricName.name,
        m.metricName.tags.asScala.toList.map(_._1).sorted
      )
  }

  case class MetricSource[F[_]](
      metric: KafkaMetric,
      name: String,
      additionalTags: Map[String, String] = Map.empty
  )(implicit F: Sync[F]) {
    val sortedTags: List[(String, String)] =
      (metric.metricName.tags.asScala ++ additionalTags).toList
        .map { case (k, v) => (underscore(k), v) }
        .sortBy(_._1)
    val labelNames: List[String] = sortedTags.map(_._1)
    val labels: List[String] = sortedTags.map(_._2)
    val help: String = Option(metric.metricName.description)
      .filter(_.trim.nonEmpty)
      .getOrElse("Kafka client metric (no description specified)")
    def value: F[Double] =
      F.delay(metric.metricValue.toString.toDouble).recover { case _ => 0 } //TODO can probably do better than this...

    def matches(other: KafkaMetric): Boolean =
      metric.metricName() === other.metricName()

    def createGauge(registry: CollectorRegistry): F[Gauge] =
      F.delay(Gauge.build().name(name).help(help).labelNames(labelNames: _*).register(registry))
    def createCounter(registry: CollectorRegistry): F[Counter] =
      F.delay(Counter.build().name(name).help(help).labelNames(labelNames: _*).register(registry))
  }

  implicit val metricNameEq: Eq[MetricName] = Eq.fromUniversalEquals

  sealed trait `Removed?`[+F[_]]
  object `Removed?` {
    object NotThere extends `Removed?`[Nothing]
    def notThere[F[_]]: `Removed?`[F] = NotThere
    object LastOne extends `Removed?`[Nothing]
    def lastOne[F[_]]: `Removed?`[F] = LastOne
    case class Removed[F[_]](
      updated: MetricAdapter[F]
    ) extends `Removed?`[F]
    def removed[F[_]](updated: MetricAdapter[F]): `Removed?`[F] =
      Removed(updated)
  }

  sealed trait MetricAdapter[F[_]] {
    def update: F[Unit]
    def add(m: MetricSource[F]): MetricAdapter[F]
    def collector: Collector
    def remove(metric: KafkaMetric): `Removed?`[F]
  }

  object MetricAdapter {
    private case class Impl[F[_]: Applicative](
      metrics: NonEmptyList[MetricSource[F]],
      collector: Collector,
      update1: MetricSource[F] => F[Unit]
    ) extends MetricAdapter[F] {
      override def add(m: MetricSource[F]): MetricAdapter[F] =
        copy(metrics = metrics :+ m)

      override def update: F[Unit] =
        metrics.traverse_(update1)

      override def remove(metric: KafkaMetric): `Removed?`[F] =
        NonEmptyList.fromList(
          metrics.filterNot(_.matches(metric))
        ).fold(`Removed?`.lastOne[F])(ms =>
          if (metrics.length === ms.length)
          /*then*/ `Removed?`.notThere[F]
          else `Removed?`.removed(Impl(ms, collector, update1))
        )
    }

    def gauge[F[_]: Sync](
      metric: MetricSource[F],
      gauge: Gauge
    ): MetricAdapter[F] =
      Impl(
        NonEmptyList.one(metric),
        gauge,
        m => m.value.flatMap(v => Sync[F].delay(gauge.labels(m.labels: _*).set(v)))
      )

    def counter[F[_]: Sync](
      metric: MetricSource[F],
      counter: Counter
    ): MetricAdapter[F] =
      Impl(
        NonEmptyList.one(metric),
        counter,
        m =>
          m.value.flatMap(
            v =>
              Sync[F].delay(
                // NOTE Should always be positive, but protect against negative
                // TODO might want to log on negative?
                counter.labels(m.labels: _*).inc(max(0, v - counter.labels(m.labels: _*).get))
              )
          )
      )
  }

  abstract class PrometheusMetricsReporterApi[F[_]](
      protected val prefix: String,
      protected val adapters: Ref[F, Map[String, MetricAdapter[F]]],
      protected val updating: SignallingRef[F, Boolean],
      protected val updatePeriod: FiniteDuration,
      private val collectorRegistry: CollectorRegistry,
  )(implicit F: Concurrent[F], T: Timer[F])
      extends MetricsReporterApi[F] {

    override def remove(metric: KafkaMetric): F[Unit] =
      adapters.modify { adapterMap =>
        adapterMap.collectFirst(kv => kv._2.remove(metric) match {
          case `Removed?`.LastOne => (adapterMap - kv._1, kv._2.collector.some)
          case `Removed?`.Removed(updated) => (adapterMap.updated(kv._1, updated), none)
        }).getOrElse((adapterMap, none[Collector]))
      }.flatMap(_.traverse_(c => F.delay(collectorRegistry.unregister(c))))

    def updateMetricsPeriodically(implicit T: Timer[F]): Stream[F, Unit] =
      for {
        _ <- Stream.eval(updating.set(true))
        _ <- Stream.eval(log.debug(s"Updating ${prefix} Prometheus metrics every ${updatePeriod}"))
        _ <- Stream
          .awakeEvery[F](updatePeriod)
          .evalMap(_ => adapters.get.flatMap(_.values.toList.traverse_(_.update)))
          .interruptWhen(updating.map(!_))
          .onFinalize(log.debug(s"Stopped updating ${prefix} Prometheus metrics"))
      } yield ()

    override def init(metrics: List[KafkaMetric]): F[Unit] =
      metrics.traverse_(add) *> F.start(updateMetricsPeriodically.compile.drain).void

    override def configure(configs: Map[String, Any]): F[Unit] = F.unit

    override def close: F[Unit] =
      adapters.modify { adapterMap =>
        (Map.empty, adapterMap.values.map(_.collector).toList)
      }.flatMap(_.traverse_(c => F.delay(collectorRegistry.unregister(c)))) *>
      updating.set(false)

    val ignore = F.unit

    def tryAdapter(
        metric: KafkaMetric,
        name: String,
        additionalTags: Map[String, String],
        create: MetricSource[F] => F[MetricAdapter[F]]
    ): F[Unit] =
      for {
        name <- s"${prefix}_${name}".pure[F]
        source = MetricSource(metric, name, additionalTags)
        maybeAdapter <- adapters.get.map(_.get(name))
        adapter <- maybeAdapter.fold[F[MetricAdapter[F]]](create(source))(_.add(source).pure[F])
        _ <- adapters.update(_ + (name -> adapter))
      } yield ()

    def adapter(
        metric: KafkaMetric,
        name: String,
        additionalTags: Map[String, String],
        create: MetricSource[F] => F[MetricAdapter[F]]
    ): F[Unit] =
      // TODO Name thread? Scheduler(1, threadPrefix = s"${prefix}_prometheus_adapter").flatMap(_.retry(
      Stream
        .retry(
          delay = 100.millis,
          nextDelay = identity,
          maxAttempts = 5,
          fo = tryAdapter(metric, name, additionalTags, create)
        )
        .compile
        .drain
  }

  def producer[F[_]](
      adapters: Ref[F, Map[String, MetricAdapter[F]]],
      updating: SignallingRef[F, Boolean],
      registry: CollectorRegistry,
      updatePeriod: FiniteDuration
  )(implicit F: Concurrent[F], T: Timer[F]): MetricsReporterApi[F] =
    new PrometheusMetricsReporterApi[F]("kafka_producer", adapters, updating, updatePeriod, registry) {

      override def add(metric: KafkaMetric): F[Unit] = {

        def gauge(name: String): F[Unit] =
          adapter(
            metric,
            name,
            Map.empty,
            source => source.createGauge(registry).map(MetricAdapter.gauge(source, _))
          )

        def counter(name: String): F[Unit] =
          adapter(
            metric,
            name,
            Map.empty,
            source => source.createCounter(registry).map(MetricAdapter.counter(source, _))
          )

        MetricId(metric) match {
          case MetricId("producer-metrics", "batch-size-avg", List("client-id")) =>
            gauge("batch_size_avg")
          case MetricId("producer-metrics", "batch-size-max", List("client-id")) =>
            gauge("batch_size_max")
          case MetricId("producer-metrics", "batch-split-total", List("client-id")) =>
            counter("batch_split_total")
          case MetricId("producer-metrics", "bufferpool-wait-ratio", List("client-id")) =>
            gauge("bufferpool_wait_ratio")
          case MetricId("producer-metrics", "bufferpool-wait-time-total", List("client-id")) =>
            gauge("bufferpool_wait_time_total") //TODO should this be a counter?
          case MetricId("producer-metrics", "buffer-available-bytes", List("client-id")) =>
            gauge("buffer_available_bytes")
          case MetricId("producer-metrics", "buffer-exhausted-total", List("client-id")) =>
            counter("buffer_exhausted_total")
          case MetricId("producer-metrics", "buffer-total-bytes", List("client-id")) =>
            gauge("buffer_total_bytes")
          case MetricId("producer-topic-metrics", "byte-total", List("client-id", "topic")) =>
            counter("topic_byte_total")
          case MetricId("producer-metrics", "compression-rate-avg", List("client-id")) =>
            gauge("compression_rate_avg")
          case MetricId("producer-topic-metrics", "compression-rate", List("client-id", "topic")) =>
            gauge("topic_compression_rate_avg")
          case MetricId("producer-metrics", "connection-close-total", List("client-id")) =>
            counter("connection_close_total") //12
          case MetricId("producer-metrics", "connection-count", List("client-id")) =>
            gauge("connection_count")
          case MetricId("producer-metrics", "connection-creation-total", List("client-id")) =>
            counter("connection_creation_total")
          case MetricId("producer-metrics", "failed-authentication-total", List("client-id")) =>
            counter("failed_authentication_total")
          case MetricId("producer-metrics", "incoming-byte-total", List("client-id")) =>
            counter("incoming_byte_total")
          case MetricId(
              "producer-node-metrics",
              "incoming-byte-total",
              List("client-id", "node-id")
              ) =>
            counter("node_incoming_byte_total")
          case MetricId("producer-metrics", "io-ratio", List("client-id")) => gauge("io_ratio")
          case MetricId("producer-metrics", "io-time-ns-avg", List("client-id")) =>
            gauge("io_time_ns_avg")
          case MetricId("producer-metrics", "iotime-total", List("client-id")) =>
            counter("iotime_total") //TODO is this really a counter, and not a gauge?
          case MetricId("producer-metrics", "io-wait-ratio", List("client-id")) =>
            gauge("io_wait_ratio")
          case MetricId("producer-metrics", "io-wait-time-ns-avg", List("client-id")) =>
            gauge("io_wait_time_ns_avg")
          case MetricId("producer-metrics", "io-waittime-total", List("client-id")) =>
            counter("io_waittime_total") //TODO really a counter?
          case MetricId("producer-metrics", "metadata-age", List("client-id")) =>
            gauge("metadata_age")
          case MetricId("producer-metrics", "network-io-total", List("client-id")) =>
            counter("network_io_total")
          case MetricId("producer-metrics", "outgoing-byte-total", List("client-id")) =>
            counter("outgoing_byte_total")
          case MetricId(
              "producer-node-metrics",
              "outgoing-byte-total",
              List("client-id", "node-id")
              ) =>
            counter("node_outgoing_byte_total")
          case MetricId("producer-metrics", "produce-throttle-time-avg", List("client-id")) =>
            gauge("produce_throttle_time_avg")
          case MetricId("producer-metrics", "produce-throttle-time-max", List("client-id")) =>
            gauge("produce_throttle_time_max")
          case MetricId("producer-metrics", "record-error-total", List("client-id")) =>
            counter("record_error_total")
          case MetricId(
              "producer-topic-metrics",
              "record-error-total",
              List("client-id", "topic")
              ) =>
            counter("topic_record_error_total")
          case MetricId("producer-metrics", "record-retry-total", List("client-id")) =>
            counter("record_retry_total")
          case MetricId(
              "producer-topic-metrics",
              "record-retry-total",
              List("client-id", "topic")
              ) =>
            counter("topic_record_retry_total")
          case MetricId("producer-metrics", "record-send-total", List("client-id")) =>
            counter("record_send_total")
          case MetricId(
              "producer-topic-metrics",
              "record-send-total",
              List("client-id", "topic")
              ) =>
            counter("topic_record_send_total")
          case MetricId("producer-metrics", "record-size-max", List("client-id")) =>
            gauge("record_size_max")
          case MetricId("producer-metrics", "record-size-avg", List("client-id")) =>
            gauge("record_size_avg")
          case MetricId("producer-metrics", "record-queue-time-avg", List("client-id")) =>
            gauge("record_queue_time_avg")
          case MetricId("producer-metrics", "record-queue-time-max", List("client-id")) =>
            gauge("record_queue_time_max")
          case MetricId("producer-metrics", "records-per-request-avg", List("client-id")) =>
            gauge("records_per_request_avg")
          case MetricId("producer-metrics", "request-total", List("client-id")) =>
            counter("request_total")
          case MetricId("producer-node-metrics", "request-total", List("client-id", "node-id")) =>
            counter("node_request_total")
          case MetricId("producer-metrics", "request-size-avg", List("client-id")) =>
            gauge("request_size_avg")
          case MetricId("producer-metrics", "request-size-max", List("client-id")) =>
            gauge("request_size_max")
          case MetricId(
              "producer-node-metrics",
              "request-size-avg",
              List("client-id", "node-id")
              ) =>
            gauge("node_request_size_avg")
          case MetricId(
              "producer-node-metrics",
              "request-size-max",
              List("client-id", "node-id")
              ) =>
            gauge("node_request_size_max")
          case MetricId("producer-metrics", "response-total", List("client-id")) =>
            counter("response_total")
          case MetricId("producer-node-metrics", "response-total", List("client-id", "node-id")) =>
            counter("node_response_total")
          case MetricId("producer-metrics", "request-latency-avg", List("client-id")) =>
            gauge("request_latency_avg")
          case MetricId("producer-metrics", "request-latency-max", List("client-id")) =>
            gauge("request_latency_max")
          case MetricId(
              "producer-node-metrics",
              "request-latency-avg",
              List("client-id", "node-id")
              ) =>
            gauge("node_request_latency_avg")
          case MetricId(
              "producer-node-metrics",
              "request-latency-max",
              List("client-id", "node-id")
              ) =>
            gauge("node_request_latency_max")
          case MetricId("producer-metrics", "requests-in-flight", List("client-id")) =>
            gauge("requests_in_flight")
          case MetricId("producer-metrics", "select-total", List("client-id")) =>
            counter("select_total")
          case MetricId("producer-metrics", "successful-authentication-total", List("client-id")) =>
            counter("successful_authentication_total")
          case MetricId("producer-metrics", "waiting-threads", List("client-id")) =>
            gauge("waiting_threads")
          case MetricId("kafka-metrics-count", "count", _) => ignore
          case MetricId("app-info", "version", _) => ignore
          case MetricId("app-info", "commit-id", _) => ignore
          case MetricId("app-info", "start-time-ms", _) => ignore
          case MetricId("producer-metrics", "batch-split-rate", _) => ignore
          case MetricId("producer-metrics", "buffer-exhausted-rate", _) => ignore
          case MetricId("producer-topic-metrics", "byte-rate", _) => ignore
          case MetricId("producer-metrics", "connection-close-rate", _) => ignore
          case MetricId("producer-metrics", "connection-creation-rate", _) => ignore
          case MetricId("producer-metrics", "failed-authentication-rate", _) => ignore
          case MetricId("producer-metrics", "incoming-byte-rate", _) => ignore
          case MetricId("producer-node-metrics", "incoming-byte-rate", _) => ignore
          case MetricId("producer-metrics", "network-io-rate", _) => ignore
          case MetricId("producer-metrics", "outgoing-byte-rate", _) => ignore
          case MetricId("producer-node-metrics", "outgoing-byte-rate", _) => ignore
          case MetricId("producer-metrics", "record-error-rate", _) => ignore
          case MetricId("producer-topic-metrics", "record-error-rate", _) => ignore
          case MetricId("producer-metrics", "record-retry-rate", _) => ignore
          case MetricId("producer-topic-metrics", "record-retry-rate", _) => ignore
          case MetricId("producer-metrics", "record-send-rate", _) => ignore
          case MetricId("producer-topic-metrics", "record-send-rate", _) => ignore
          case MetricId("producer-metrics", "request-rate", _) => ignore
          case MetricId("producer-node-metrics", "request-rate", _) => ignore
          case MetricId("producer-metrics", "response-rate", _) => ignore
          case MetricId("producer-node-metrics", "response-rate", _) => ignore
          case MetricId("producer-metrics", "select-rate", _) => ignore
          case MetricId("producer-metrics", "successful-authentication-rate", _) => ignore

          // New in 2.2.1
          // TODO Decide if we want to use any of the following metrics
          case MetricId(
              "producer-metrics",
              "successful-reauthentication-total",
              List("client-id")
              ) =>
            ignore
          case MetricId(
              "producer-metrics",
              "successful-reauthentication-rate",
              List("client-id")
              ) =>
            ignore
          case MetricId(
              "producer-metrics",
              "successful-authentication-no-reauth-total",
              List("client-id")
              ) =>
            ignore
          case MetricId("producer-metrics", "failed-reauthentication-total", List("client-id")) =>
            ignore
          case MetricId("producer-metrics", "failed-reauthentication-rate", List("client-id")) =>
            ignore
          case MetricId("producer-metrics", "reauthentication-latency-max", List("client-id")) =>
            ignore
          case MetricId("producer-metrics", "reauthentication-latency-avg", List("client-id")) =>
            ignore

          case id =>
            log.error(
              s"Could not create Prometheus collector for unknown Kafka producer metric: $id"
            )
        }
      }
    }

  def consumer[F[_]](
      adapters: Ref[F, Map[String, MetricAdapter[F]]],
      updating: SignallingRef[F, Boolean],
      registry: CollectorRegistry,
      updatePeriod: FiniteDuration
  )(implicit F: Concurrent[F], T: Timer[F]): MetricsReporterApi[F] =
    new PrometheusMetricsReporterApi[F]("kafka_consumer", adapters, updating, updatePeriod, registry) {

      override def add(metric: KafkaMetric): F[Unit] = {

        def gauge(name: String, additionalTags: Map[String, String] = Map.empty): F[Unit] =
          adapter(
            metric,
            name,
            additionalTags,
            source => source.createGauge(registry).map(MetricAdapter.gauge(source, _))
          )

        def counter(name: String): F[Unit] =
          adapter(
            metric,
            name,
            Map.empty,
            source => source.createCounter(registry).map(MetricAdapter.counter(source, _))
          )

        MetricId(metric) match {
          case MetricId("consumer-coordinator-metrics", "assigned-partitions", List("client-id")) =>
            gauge("assigned_partitions")
          case MetricId(
              "consumer-fetch-manager-metrics",
              "bytes-consumed-total",
              List("client-id")
              ) =>
            counter("bytes_consumed_total")
          case MetricId(
              "consumer-fetch-manager-metrics",
              "bytes-consumed-total",
              List("client-id", "topic")
              ) =>
            counter("topic_bytes_consumed_total")
          case MetricId("consumer-coordinator-metrics", "commit-latency-avg", List("client-id")) =>
            gauge("commit_latency_avg")
          case MetricId("consumer-coordinator-metrics", "commit-latency-max", List("client-id")) =>
            gauge("commit_latency_max")
          case MetricId("consumer-coordinator-metrics", "commit-total", List("client-id")) =>
            counter("commit_total")
          case MetricId("consumer-metrics", "connection-count", List("client-id")) =>
            gauge("connection_count")
          case MetricId("consumer-metrics", "connection-close-total", List("client-id")) =>
            counter("connection_close_total")
          case MetricId("consumer-metrics", "connection-creation-total", List("client-id")) =>
            counter("connection_creation_total")
          case MetricId("consumer-metrics", "failed-authentication-total", List("client-id")) =>
            counter("failed_authentication_total")
          case MetricId("consumer-fetch-manager-metrics", "fetch-latency-avg", List("client-id")) =>
            gauge("fetch_latency_avg")
          case MetricId("consumer-fetch-manager-metrics", "fetch-latency-max", List("client-id")) =>
            gauge("fetch_latency_max")
          case MetricId("consumer-fetch-manager-metrics", "fetch-total", List("client-id")) =>
            counter("fetch_total")
          case MetricId("consumer-fetch-manager-metrics", "fetch-size-avg", List("client-id")) =>
            gauge("fetch_size_avg")
          case MetricId(
              "consumer-fetch-manager-metrics",
              "fetch-size-avg",
              List("client-id", "topic")
              ) =>
            gauge("topic_fetch_size_avg")
          case MetricId("consumer-fetch-manager-metrics", "fetch-size-max", List("client-id")) =>
            gauge("fetch_size_max")
          case MetricId(
              "consumer-fetch-manager-metrics",
              "fetch-size-max",
              List("client-id", "topic")
              ) =>
            gauge("topic_fetch_size_max")
          case MetricId(
              "consumer-fetch-manager-metrics",
              "fetch-throttle-time-avg",
              List("client-id")
              ) =>
            gauge("fetch_throttle_time_avg")
          case MetricId(
              "consumer-fetch-manager-metrics",
              "fetch-throttle-time-max",
              List("client-id")
              ) =>
            gauge("fetch_throttle_time_max")
          case MetricId(
              "consumer-coordinator-metrics",
              "heartbeat-response-time-max",
              List("client-id")
              ) =>
            gauge("heartbeat_response_time_max")
          case MetricId("consumer-coordinator-metrics", "heartbeat-total", List("client-id")) =>
            counter("heartbeat_total")
          case MetricId("consumer-metrics", "incoming-byte-total", List("client-id")) =>
            counter("incoming_byte_total")
          case MetricId(
              "consumer-node-metrics",
              "incoming-byte-total",
              List("client-id", "node-id")
              ) =>
            counter("node_incoming_byte_total")
          case MetricId("consumer-metrics", "io-wait-time-ns-avg", List("client-id")) =>
            gauge("io_wait_time_ns_avg")
          case MetricId("consumer-metrics", "io-waittime-total", List("client-id")) =>
            counter("io_waittime_total") //TODO really a counter?
          case MetricId("consumer-metrics", "io-wait-ratio", List("client-id")) =>
            gauge("io_wait_ratio")
          case MetricId("consumer-metrics", "io-time-ns-avg", List("client-id")) =>
            gauge("io_time_ns_avg")
          case MetricId("consumer-metrics", "iotime-total", List("client-id")) =>
            counter("iotime_total") //TODO is this really a counter, and not a gauge?
          case MetricId("consumer-metrics", "io-ratio", List("client-id")) => gauge("io_ratio")
          case MetricId("consumer-coordinator-metrics", "join-time-avg", List("client-id")) =>
            gauge("join_time_avg")
          case MetricId("consumer-coordinator-metrics", "join-time-max", List("client-id")) =>
            gauge("join_time_max")
          case MetricId("consumer-coordinator-metrics", "join-total", List("client-id")) =>
            counter("join_total")
          case MetricId(
              "consumer-coordinator-metrics",
              "last-heartbeat-seconds-ago",
              List("client-id")
              ) =>
            gauge("last_heartbeat_seconds_ago")
          case MetricId("consumer-metrics", "network-io-total", List("client-id")) =>
            counter("network_io_total")
          case MetricId("consumer-metrics", "outgoing-byte-total", List("client-id")) =>
            counter("outgoing_byte_total")
          case MetricId(
              "consumer-node-metrics",
              "outgoing-byte-total",
              List("client-id", "node-id")
              ) =>
            counter("node_outgoing_byte_total")
          case MetricId(
              "consumer-fetch-manager-metrics",
              "records-per-request-avg",
              List("client-id")
              ) =>
            gauge("records_per_request_avg")
          case MetricId(
              "consumer-fetch-manager-metrics",
              "records-per-request-avg",
              List("client-id", "topic")
              ) =>
            gauge("topic_records_per_request_avg")
          case MetricId(
              "consumer-fetch-manager-metrics",
              "records-consumed-total",
              List("client-id")
              ) =>
            counter("records_consumed_total")
          case MetricId(
              "consumer-fetch-manager-metrics",
              "records-consumed-total",
              List("client-id", "topic")
              ) =>
            counter("topic_records_consumed_total")
          case MetricId(
              "consumer-fetch-manager-metrics",
              "records-lag",
              List("client-id", "partition", "topic")
              ) =>
            gauge("topic_records_lag")
          case MetricId(
              "consumer-fetch-manager-metrics",
              "records-lag-avg",
              List("client-id", "partition", "topic")
              ) =>
            gauge("topic_records_lag_avg")
          case MetricId("consumer-fetch-manager-metrics", "records-lag-max", List("client-id")) =>
            gauge("records_lag_max")
          case MetricId(
              "consumer-fetch-manager-metrics",
              "records-lag-max",
              List("client-id", "partition", "topic")
              ) =>
            gauge("topic_records_lag_max")
          case MetricId(
              "consumer-fetch-manager-metrics",
              "records-lead",
              List("client-id", "partition", "topic")
              ) =>
            gauge("topic_records_lead")
          case MetricId(
              "consumer-fetch-manager-metrics",
              "records-lead-avg",
              List("client-id", "partition", "topic")
              ) =>
            gauge("topic_records_lead_avg")
          case MetricId("consumer-fetch-manager-metrics", "records-lead-min", List("client-id")) =>
            gauge("records_lead_min")
          case MetricId(
              "consumer-fetch-manager-metrics",
              "records-lead-min",
              List("client-id", "partition", "topic")
              ) =>
            gauge("topic_records_lead_min")
          case MetricId("consumer-metrics", "request-total", List("client-id")) =>
            counter("request_total")
          case MetricId("consumer-node-metrics", "request-total", List("client-id", "node-id")) =>
            counter("node_request_total")
          case MetricId("consumer-metrics", "request-size-avg", List("client-id")) =>
            gauge("request_size_avg")
          case MetricId(
              "consumer-node-metrics",
              "request-size-avg",
              List("client-id", "node-id")
              ) =>
            gauge("node_request_size_avg")
          case MetricId("consumer-metrics", "request-size-max", List("client-id")) =>
            gauge("request_size_max")
          case MetricId(
              "consumer-node-metrics",
              "request-size-max",
              List("client-id", "node-id")
              ) =>
            gauge("node_request_size_max")
          case MetricId(
              "consumer-node-metrics",
              "request-latency-avg",
              List("client-id", "node-id")
              ) =>
            gauge("node_request_latency_avg")
          case MetricId(
              "consumer-node-metrics",
              "request-latency-max",
              List("client-id", "node-id")
              ) =>
            gauge("node_request_latency_max")
          case MetricId("consumer-metrics", "response-total", List("client-id")) =>
            counter("response_total")
          case MetricId("consumer-node-metrics", "response-total", List("client-id", "node-id")) =>
            counter("node_response_total")
          case MetricId("consumer-metrics", "select-total", List("client-id")) =>
            counter("select_total")
          case MetricId("consumer-metrics", "successful-authentication-total", List("client-id")) =>
            counter("successful_authentication_total")
          case MetricId("consumer-coordinator-metrics", "sync-time-avg", List("client-id")) =>
            gauge("sync_time_avg")
          case MetricId("consumer-coordinator-metrics", "sync-time-max", List("client-id")) =>
            gauge("sync_time_max")
          case MetricId("consumer-coordinator-metrics", "sync-total", List("client-id")) =>
            counter("sync_total")
          case MetricId("kafka-metrics-count", "count", _) => ignore
          case MetricId("app-info", "version", _) => ignore
          case MetricId("app-info", "commit-id", _) => ignore
          case MetricId("app-info", "start-time-ms", _) => ignore
          case MetricId("consumer-fetch-manager-metrics", "bytes-consumed-rate", _) => ignore
          case MetricId("consumer-coordinator-metrics", "commit-rate", _) => ignore
          case MetricId("consumer-metrics", "connection-close-rate", _) => ignore
          case MetricId("consumer-metrics", "connection-creation-rate", _) => ignore
          case MetricId("consumer-metrics", "failed-authentication-rate", _) => ignore
          case MetricId("consumer-fetch-manager-metrics", "fetch-rate", _) => ignore
          case MetricId("consumer-coordinator-metrics", "heartbeat-rate", _) => ignore
          case MetricId("consumer-metrics", "incoming-byte-rate", _) => ignore
          case MetricId("consumer-node-metrics", "incoming-byte-rate", _) => ignore
          case MetricId("consumer-coordinator-metrics", "join-rate", _) => ignore
          case MetricId("consumer-metrics", "network-io-rate", _) => ignore
          case MetricId("consumer-metrics", "outgoing-byte-rate", _) => ignore
          case MetricId("consumer-node-metrics", "outgoing-byte-rate", _) => ignore
          case MetricId("consumer-fetch-manager-metrics", "records-consumed-rate", _) => ignore
          case MetricId("consumer-metrics", "request-rate", _) => ignore
          case MetricId("consumer-node-metrics", "request-rate", _) => ignore
          case MetricId("consumer-metrics", "response-rate", _) => ignore
          case MetricId("consumer-node-metrics", "response-rate", _) => ignore
          case MetricId("consumer-metrics", "select-rate", _) => ignore
          case MetricId("consumer-metrics", "successful-authentication-rate", _) => ignore
          case MetricId("consumer-coordinator-metrics", "sync-rate", _) => ignore

          // New in 2.2.1.
          // TODO Decide if we want to use any of the following metrics
          case MetricId(
              "consumer-metrics",
              "successful-reauthentication-total",
              List("client-id")
              ) =>
            ignore
          case MetricId(
              "consumer-metrics",
              "successful-reauthentication-rate",
              List("client-id")
              ) =>
            ignore
          case MetricId(
              "consumer-metrics",
              "successful-authentication-no-reauth-total",
              List("client-id")
              ) =>
            ignore
          case MetricId("consumer-metrics", "failed-reauthentication-total", List("client-id")) =>
            ignore
          case MetricId("consumer-metrics", "failed-reauthentication-rate", List("client-id")) =>
            ignore
          case MetricId("consumer-metrics", "reauthentication-latency-max", List("client-id")) =>
            ignore
          case MetricId("consumer-metrics", "reauthentication-latency-avg", List("client-id")) =>
            ignore
          case MetricId(
              "consumer-metrics",
              "successful-reauthentication-total",
              List("client-id")
              ) =>
            ignore
          case MetricId(
              "consumer-metrics",
              "successful-reauthentication-rate",
              List("client-id")
              ) =>
            ignore
          case MetricId(
              "consumer-metrics",
              "successful-authentication-no-reauth-total",
              List("client-id")
              ) =>
            ignore
          case MetricId("consumer-metrics", "failed-reauthentication-total", List("client-id")) =>
            ignore
          case MetricId("consumer-metrics", "failed-reauthentication-rate", List("client-id")) =>
            ignore
          case MetricId("consumer-metrics", "reauthentication-latency-max", List("client-id")) =>
            ignore
          case MetricId("consumer-metrics", "reauthentication-latency-avg", List("client-id")) =>
            ignore

          // New in 2.4.1.
          // TODO Decide if we want to use any of the following metrics
          case MetricId(
              "consumer-fetch-manager-metrics",
              "preferred-read-replica",
              List("client-id", "partition", "topic")
              ) =>
            ignore
          case MetricId("consumer-metrics", "poll-idle-ratio-avg", List("client-id")) =>
            ignore
          case MetricId("consumer-metrics", "time-between-poll-max", List("client-id")) =>
            ignore
          case MetricId("consumer-metrics", "time-between-poll-avg", List("client-id")) =>
            ignore
          case MetricId("consumer-metrics", "last-poll-seconds-ago", List("client-id")) =>
            ignore
          case MetricId(
              "consumer-coordinator-metrics",
              "partition-lost-latency-max",
              List("client-id")
              ) =>
            ignore
          case MetricId(
              "consumer-coordinator-metrics",
              "partition-lost-latency-avg",
              List("client-id")
              ) =>
            ignore
          case MetricId(
              "consumer-coordinator-metrics",
              "partition-assigned-latency-max",
              List("client-id")
              ) =>
            ignore
          case MetricId(
              "consumer-coordinator-metrics",
              "partition-assigned-latency-avg",
              List("client-id")
              ) =>
            ignore
          case MetricId(
              "consumer-coordinator-metrics",
              "partition-revoked-latency-max",
              List("client-id")
              ) =>
            ignore
          case MetricId(
              "consumer-coordinator-metrics",
              "partition-revoked-latency-avg",
              List("client-id")
              ) =>
            ignore
          case MetricId("consumer-coordinator-metrics", "rebalance-total", List("client-id")) =>
            ignore
          case MetricId(
              "consumer-coordinator-metrics",
              "rebalance-rate-per-hour",
              List("client-id")
              ) =>
            ignore
          case MetricId(
              "consumer-coordinator-metrics",
              "rebalance-latency-max",
              List("client-id")
              ) =>
            ignore
          case MetricId(
              "consumer-coordinator-metrics",
              "rebalance-latency-total",
              List("client-id")
              ) =>
            ignore
          case MetricId(
              "consumer-coordinator-metrics",
              "rebalance-latency-avg",
              List("client-id")
              ) =>
            ignore
          case MetricId(
              "consumer-coordinator-metrics",
              "last-rebalance-seconds-ago",
              List("client-id")
              ) =>
            ignore
          case MetricId(
              "consumer-coordinator-metrics",
              "failed-rebalance-rate-per-hour",
              List("client-id")
              ) =>
            ignore
          case MetricId(
              "consumer-coordinator-metrics",
              "failed-rebalance-total",
              List("client-id")
              ) =>
            ignore

          case id =>
            log.error(
              s"Could not create Prometheus collector for unknown Kafka consumer metric: $id"
            )
        }
      }
    }

  val defaultUpdatePeriod = 10 seconds

  def producer[F[_]: Concurrent: Timer](
      registry: CollectorRegistry = CollectorRegistry.defaultRegistry,
      updatePeriod: FiniteDuration = defaultUpdatePeriod
  ): F[MetricsReporterApi[F]] =
    for {
      adapters <- Ref.of[F, Map[String, MetricAdapter[F]]](Map.empty)
      updating <- SignallingRef[F, Boolean](false)
    } yield producer[F](adapters, updating, registry, updatePeriod)

  def consumer[F[_]: Concurrent: Timer](
      registry: CollectorRegistry = CollectorRegistry.defaultRegistry,
      updatePeriod: FiniteDuration = defaultUpdatePeriod
  ): F[MetricsReporterApi[F]] =
    for {
      adapters <- Ref.of[F, Map[String, MetricAdapter[F]]](Map.empty)
      updating <- SignallingRef[F, Boolean](false)
    } yield consumer[F](adapters, updating, registry, updatePeriod)
}
