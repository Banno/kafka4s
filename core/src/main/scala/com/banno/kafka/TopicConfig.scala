package com.banno.kafka

import cats._
import cats.syntax.all._
import org.apache.kafka.common.config.TopicConfig._
import scala.concurrent.duration._

sealed trait CleanupPolicy

sealed trait TopicConfig {
  def toMap: Map[String, String]
}

object TopicConfig {
  private final case class CleanupPolicyImpl(
      stringValue: String
  ) extends CleanupPolicy

  val compact: CleanupPolicy = CleanupPolicyImpl(CLEANUP_POLICY_COMPACT)

  val delete: CleanupPolicy = CleanupPolicyImpl(CLEANUP_POLICY_DELETE)

  private final case class Impl(
      toMap: Map[String, String]
  ) extends TopicConfig

  private def of(key: String, value: String): Impl =
    Impl(Map(key -> value))

  // TODO do we need to make sure that
  // cleanupPolicy(compact) |+| cleanupPolicy(delete) === cleanupPolicy(compact, delete)?
  def cleanupPolicy(policies: CleanupPolicy*): TopicConfig = {
    val p = policies.map { case CleanupPolicyImpl(s) => s }.distinct.intercalate(",")
    of(CLEANUP_POLICY_CONFIG, p)
  }

  def retention(duration: FiniteDuration): TopicConfig =
    of(RETENTION_MS_CONFIG, duration.toMillis.toString())

  def infiniteRetention: TopicConfig =
    of(RETENTION_MS_CONFIG, "-1")

  def minCleanableDirtyRatio(ratio: Double): TopicConfig =
    of(MIN_CLEANABLE_DIRTY_RATIO_CONFIG, ratio.toString)

  def segmentBytes(bytes: Int): TopicConfig =
    of(SEGMENT_BYTES_CONFIG, bytes.toString())

  def segmentKilobytes(kb: Int): TopicConfig =
    segmentBytes(kb * 1024)

  def segmentMegabytes(mb: Int): TopicConfig =
    segmentKilobytes(mb * 1024)

  def segmentGigabytes(gb: Int): TopicConfig =
    segmentMegabytes(gb * 1024)

  def segmentDuration(duration: FiniteDuration): TopicConfig =
    of(SEGMENT_MS_CONFIG, duration.toMillis.toString())

  // Technically this admits of a monus as well, but not sure we have any use
  // for it.
  implicit val topicConfigMonoid: Monoid[TopicConfig] =
    new Monoid[TopicConfig] {
      def empty: TopicConfig = Impl(Map.empty)

      def combine(x: TopicConfig, y: TopicConfig): TopicConfig =
        Impl(x.toMap ++ y.toMap)
    }
}
