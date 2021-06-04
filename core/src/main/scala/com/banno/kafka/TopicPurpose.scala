package com.banno.kafka

import scala.concurrent.duration._

import cats.syntax.all._

sealed trait TopicContentType
object TopicContentType {
  object Events extends TopicContentType
  object State extends TopicContentType
  object Commands extends TopicContentType
  object Ephemera extends TopicContentType
  object Negligible extends TopicContentType
}

sealed trait TopicPurpose {
  def partitions: Int
  def replicationFactor: Short
  def configs: TopicConfig
  def contentType: TopicContentType
}

object TopicPurpose {
  import TopicConfig._

  private final class Impl(
      val partitions: Int,
      val configs: TopicConfig,
      val contentType: TopicContentType,
      val replicationFactor: Short,
  ) extends TopicPurpose

  private object Impl {
    def apply(
        partitions: Int,
        configs: TopicConfig,
        contentType: TopicContentType,
    ): Impl =
      new Impl(
        partitions,
        configs,
        contentType,
        contentType match {
          case TopicContentType.Ephemera => 2.toShort
          case TopicContentType.Negligible => 1.toShort
          case _ => 3.toShort
        }
      )
  }

  def mediumScale(configs: TopicConfig, contentType: TopicContentType): TopicPurpose =
    Impl(partitions = 5, configs, contentType)

  /** If the topic is small, there is not much need to scale it, so the
    * simplicity of one partition should suffice.
    */
  def lowScale(configs: TopicConfig, contentType: TopicContentType): TopicPurpose =
    Impl(partitions = 1, configs, contentType)

  /** A state topic by default doesn't need to keep history; compact
    */
  val smallState: TopicPurpose =
    lowScale(
      cleanupPolicy(compact) |+|
      minCleanableDirtyRatio(0.01) |+|
      segmentMegabytes(1) |+|
      segmentDuration(10.minutes),
      TopicContentType.State
    )

  /** A state topic by default doesn't need to keep history; compact.
    */
  val mediumState: TopicPurpose =
    lowScale(
      cleanupPolicy(compact) |+|
      minCleanableDirtyRatio(0.10) |+|
      segmentMegabytes(100) |+|
      segmentDuration(1.day),
      TopicContentType.State
    )

  /** Because this is a command topic, we do not need infinite retention. OTOH
    * if the topic is truly small, it can't hurt to err a bit on the side of
    * retaining it longer than needed, because there just won't be that much
    * data there; retain for 30 days.
    */
  val smallCommand: TopicPurpose = lowScale(retention(30.days), TopicContentType.Commands)

  /** Medium scale with infinite retention.
    */
  val event: TopicPurpose = mediumScale(infiniteRetention, TopicContentType.Events)

  /** Medium scale with one hour retention and a replication factor of only
    * two.
    */
  val ephemeron: TopicPurpose =
    mediumScale(retention(1.hour), TopicContentType.Ephemera)

  /** Low scale, 1 partition, 1 replica, 1 hour retention. For data that is negligible (e.g. test topic). */
  val negligible: TopicPurpose =
    lowScale(retention(1.hour), TopicContentType.Negligible)
}
