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
package vulcan

import cats.syntax.all.*
import com.banno.kafka.schemaregistry.*
import io.confluent.kafka.schemaregistry.CompatibilityLevel
import io.confluent.kafka.schemaregistry.{
  ParsedSchemaHolder,
  SimpleParsedSchemaHolder,
}
import munit.*
import org.scalacheck.*
import scala.jdk.CollectionConverters.*
import scala.util.*
import _root_.vulcan.*

class CodecCharacterizationTests extends ScalaCheckSuite {
  test("Vulcan record codec encodes to a generic record") {
    Prop.forAll { (foo: Foo) =>
      Codec
        .encodeGenericRecord[Try, Foo](foo)
        .isSuccess
    }
  }

  test("Vulcan union of records codec encodes to a generic record") {
    Prop.forAll { (foolike: Foolike) =>
      Codec
        .encodeGenericRecord[Try, Foolike](foolike)
        .isSuccess
    }
  }

  test(
    "Schema for any serialized member is backwards compatible with the " +
    "whole union's schema"
  ) {
    Prop.forAll { (foolike: Foolike) =>
      val trySchema = Schema.vulcan[Try, Foolike]
      val attempt = trySchema.flatMap(x => x.unparse(foolike).map(x -> _))
      assert(clue(attempt).isSuccess)
      val (schema, record) = attempt.toOption.get
      val parsedSchema = record.getSchema.asParsedSchema
      val holder: ParsedSchemaHolder =
        new SimpleParsedSchemaHolder(parsedSchema)
      val errors = schema.ast.asParsedSchema.isCompatible(
        CompatibilityLevel.BACKWARD_TRANSITIVE,
        List(holder).asJava,
      )
      assert(clue(errors).isEmpty)
    }
  }
}

trait Foolike

object Foolike {
  implicit val arb: Arbitrary[Foolike] =
    Arbitrary(
      Gen.oneOf(Arbitrary.arbitrary[Foo], Arbitrary.arbitrary[Quasifoo])
    )

  implicit val codec: Codec[Foolike] =
    Codec.union { alt =>
      alt[Foo] |+| alt[Quasifoo]
    }
}

final case class Foo(
    bar: Boolean,
    baz: Int,
) extends Foolike

object Foo {
  implicit val arb: Arbitrary[Foo] =
    Arbitrary(
      for {
        bar <- Arbitrary.arbitrary[Boolean]
        baz <- Arbitrary.arbitrary[Int]
      } yield Foo(bar, baz)
    )

  implicit val codec: Codec[Foo] =
    Codec.record(
      name = "Foo",
      namespace = "com.banno.kafka.vulcan",
    ) { field =>
      (field("bar", _.bar), field("baz", _.baz)).mapN(Foo.apply)
    }
}

final case class Quasifoo(
    qux: String
) extends Foolike

object Quasifoo {
  implicit val arb: Arbitrary[Quasifoo] =
    Arbitrary(Arbitrary.arbitrary[String].map(Quasifoo.apply))

  implicit val codec: Codec[Quasifoo] =
    Codec.record(
      name = "Quasifoo",
      namespace = "com.banno.kafka.vulcan",
    ) { field =>
      field("qux", _.qux).map(Quasifoo.apply)
    }
}
