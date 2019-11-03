package com.banno.kafka.connect

import org.scalatest._
import org.scalatestplus.scalacheck._
import com.mrdziuban.ScalacheckMagnolia._
import scala.concurrent.duration._

class StringEncoderDecoderSpec extends PropSpec with ScalaCheckDrivenPropertyChecks with Matchers {

  def roundTrip[A: StringEncoder: StringDecoder](a: A) =
    StringDecoder[A].decode(StringEncoder[A].encode(a))

  property("String") {
    forAll { a: String =>
      roundTrip(a)
    }
  }

  property("Boolean") {
    forAll { a: Boolean =>
      roundTrip(a)
    }
  }

  property("Int") {
    forAll { a: Int =>
      roundTrip(a)
    }
  }

  property("Long") {
    forAll { a: Long =>
      roundTrip(a)
    }
  }

  property("Float") {
    forAll { a: Float =>
      roundTrip(a)
    }
  }

  property("Double") {
    forAll { a: Double =>
      roundTrip(a)
    }
  }

  property("Short") {
    forAll { a: Short =>
      roundTrip(a)
    }
  }

  property("Byte") {
    forAll { a: Byte =>
      roundTrip(a)
    }
  }

  property("FiniteDuration") {
    forAll { a: FiniteDuration =>
      roundTrip(a)
    }
  }

  property("TestPartition") {
    forAll { a: TestPartition =>
      roundTrip(a)
    }
  }

  property("TestOffset") {
    forAll { a: TestOffset =>
      roundTrip(a)
    }
  }

  property("Option") {
    forAll { a: Option[String] =>
      StringEncoder[Option[String]].encode(a) should ===(a.getOrElse(null))
      roundTrip(a)
    }
  }
}
