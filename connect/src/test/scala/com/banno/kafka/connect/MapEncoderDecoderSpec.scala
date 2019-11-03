package com.banno.kafka.connect

import org.scalatest._
import org.scalatestplus.scalacheck._
import com.mrdziuban.ScalacheckMagnolia._
import scala.concurrent.duration._

class MapEncoderDecoderSpec extends PropSpec with ScalaCheckDrivenPropertyChecks with Matchers {

  property("Round-trip between MapEncoder and MapDecoder") {
    val e = MapEncoder[TestTaskConfigs]
    val d = MapDecoder[TestTaskConfigs]
    forAll { c: TestTaskConfigs =>
      d.decode(e.encode(c)) should ===(c)
    }
  }

  property("Decode missing field using default value") {
    val e = MapEncoder[TestConnectorConfigs]
    val d = MapDecoder[TestConnectorConfigs]
    forAll { c: TestConnectorConfigs =>
      val m = e.encode(c) - "c" - "d" - "e"
      d.decode(m) should ===(c.copy(c = 1 second, d = "default", e = None))
    }
  }
}
