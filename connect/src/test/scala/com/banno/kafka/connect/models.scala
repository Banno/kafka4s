package com.banno.kafka.connect

import scala.concurrent.duration._
import io.circe.generic.semiauto._
import ConfigDefEncoder.Documentation

case class TestPartition(partition: Int)
object TestPartition {
  implicit val circeEncoder = deriveEncoder[TestPartition]
  implicit val circeDecoder = deriveDecoder[TestPartition]
}

case class TestOffset(offset: String)
object TestOffset {
  implicit val circeEncoder = deriveEncoder[TestOffset]
  implicit val circeDecoder = deriveDecoder[TestOffset]
}

case class TestConnectorConfigs(
    @Documentation("initialOffset docs") initialOffset: TestOffset,
    @Documentation("a docs") a: String,
    @Documentation("b docs") b: Int,
    @Documentation("c docs") c: FiniteDuration = 1.second,
    @Documentation("d docs") d: String = "default",
    @Documentation("e docs") e: Option[String] = None
)

case class TestTaskConfigs(
    sourcePartitions: List[TestPartition],
    initialOffset: TestOffset,
    a: String,
    b: Int,
    c: FiniteDuration,
    d: String = "default",
    e: Option[String] = None
) extends TaskConfigs[TestPartition, TestOffset]
