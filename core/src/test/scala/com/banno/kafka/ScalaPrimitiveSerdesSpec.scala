package com.banno.kafka

import cats.syntax.all._
import munit._
import org.apache.kafka.common.serialization._
import org.scalacheck.Prop._

class ScalaPrimitiveSerdesSpec extends ScalaCheckSuite {
  property("Scala Double serde") {
    val s = implicitly[Serializer[Double]]
    val d = implicitly[Deserializer[Double]]
    forAll { x: Double =>
      d.deserialize("topic", s.serialize("topic", x)) === x
    }
  }

  property("Scala Float serde") {
    val s = implicitly[Serializer[Float]]
    val d = implicitly[Deserializer[Float]]
    forAll { x: Float =>
      d.deserialize("topic", s.serialize("topic", x)) === x
    }
  }

  property("Scala Int serde") {
    val s = implicitly[Serializer[Int]]
    val d = implicitly[Deserializer[Int]]
    forAll { x: Int =>
      d.deserialize("topic", s.serialize("topic", x)) === x
    }
  }

  property("Scala Long serde") {
    val s = implicitly[Serializer[Long]]
    val d = implicitly[Deserializer[Long]]
    forAll { x: Long =>
      d.deserialize("topic", s.serialize("topic", x)) === x
    }
  }

  property("Scala Short serde") {
    val s = implicitly[Serializer[Short]]
    val d = implicitly[Deserializer[Short]]
    forAll { x: Short =>
      d.deserialize("topic", s.serialize("topic", x)) === x
    }
  }

}
