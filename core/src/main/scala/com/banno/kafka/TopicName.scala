package com.banno.kafka

import cats._

sealed trait TopicName

object TopicName {
  private final case class Impl(name: String) extends TopicName {
    override def toString(): String = name
  }

  def apply(name: String): TopicName = Impl(name)

  implicit val showName: Show[TopicName] = Show.fromToString
}
