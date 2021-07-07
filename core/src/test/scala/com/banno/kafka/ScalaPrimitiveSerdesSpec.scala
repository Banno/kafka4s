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
