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

package com.banno
package kafka

import io.confluent.kafka.serializers._
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient
import org.apache.avro._
import org.apache.avro.generic.GenericRecord
import com.sksamuel.avro4s.{DefaultFieldMapper, RecordFormat, SchemaFor}
import munit._

import scala.util._
import scala.jdk.CollectionConverters._
import shapeless._

// http://avro.apache.org/docs/current/api/java/index.html
object Compatibility {

  // A new schema is backward compatible if it can read data written with the previous schema.
  // Use a strategy that validates that a schema can be used to read existing schema(s) according to the Avro default schema resolution.
  val backward = new SchemaValidatorBuilder().canReadStrategy().validateLatest()

  // A new schema is backward transitive compatible if it can read data written with all previous schemas.
  val backwardTransitive = new SchemaValidatorBuilder().canReadStrategy().validateAll()

  // A new schema is forward compatible if the previous schema can read data written with the new schema.
  // Use a strategy that validates that a schema can be read by existing schema(s) according to the Avro default schema resolution.
  val forward = new SchemaValidatorBuilder().canBeReadStrategy().validateLatest()

  // A new schema is forward transitive compatible if all previous schemas can read data written with the new schema.
  val forwardTransitive = new SchemaValidatorBuilder().canBeReadStrategy().validateAll()

  // A new schema is full compatible if it's both backward and forward compatible with the previous schema. In other words, the new schema can read data written with the previous schema, and the previous schema can read data written with the new schema.
  // Use a strategy that validates that a schema can read existing schema(s), and vice-versa, according to the Avro default schema resolution.
  val full = new SchemaValidatorBuilder().mutualReadStrategy().validateLatest()

  // A new schema is full transitive compatible if it's both backward and forward compatible with all previous schemas. In other words, the new schema can read data written with all previous schemas, and all previous schemas can read data written with the new schema.
  val fullTransitive = new SchemaValidatorBuilder().mutualReadStrategy().validateAll()

  /** If newSchema is sn, then previousSchemas should be in reverse chronological order, i.e. [s1, s2, ..., sn-1] */
  def compatible(
      validator: SchemaValidator,
      newSchema: Schema,
      previousSchemas: Seq[Schema]
  ): Boolean =
    try {
      validator.validate(newSchema, previousSchemas.reverse.asJava) // Validator checks in list order, but checks should occur in reverse chronological order
      true
    } catch {
      case _: SchemaValidationException => false
    }

  def compatible(validator: SchemaValidator, newSchema: Schema, previousSchema: Schema): Boolean =
    compatible(validator, newSchema, Seq(previousSchema))
  def backwardCompatible(newSchema: Schema, previousSchema: Schema): Boolean =
    compatible(backward, newSchema, previousSchema)
  def backwardTransitiveCompatible(newSchema: Schema, previousSchema: Schema): Boolean =
    compatible(backwardTransitive, newSchema, previousSchema)
  def forwardCompatible(newSchema: Schema, previousSchema: Schema): Boolean =
    compatible(forward, newSchema, previousSchema)
  def forwardTransitiveCompatible(newSchema: Schema, previousSchema: Schema): Boolean =
    compatible(forwardTransitive, newSchema, previousSchema)
  def fullCompatible(newSchema: Schema, previousSchema: Schema): Boolean =
    compatible(full, newSchema, previousSchema)
  def fullTransitiveCompatible(newSchema: Schema, previousSchema: Schema): Boolean =
    compatible(fullTransitive, newSchema, previousSchema)
}

class AvrohuggerAvro4sSchemaEvolutionSpec extends FunSuite {
  val client = new MockSchemaRegistryClient()
  val configs = Map(
    AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG -> "http://inmemorytest"
  ).asJava
  val serializer = new KafkaAvroSerializer(client)
  serializer.configure(configs, false)
  val deserializer = new KafkaAvroDeserializer(client)
  deserializer.configure(configs, false)

  def randomString(size: Int): String = scala.util.Random.alphanumeric.take(size).mkString
  def testTopic: String = randomString(10)

  type ABC = A :+: B :+: C :+: CNil

  test("Reordering fields is backward and forward compatible") {
    val s1 = SchemaFor[ReorderFields1].schema(DefaultFieldMapper)
    val s2 = SchemaFor[ReorderFields2].schema(DefaultFieldMapper)
    assertEquals(Compatibility.backwardCompatible(s2, s1), true)
    assertEquals(Compatibility.forwardCompatible(s2, s1), true)

    val topic = testTopic
    val rf1 = RecordFormat[ReorderFields1]
    val rf2 = RecordFormat[ReorderFields2]
    val r1 = ReorderFields1(1, "s")
    val r2 = ReorderFields2("s", 1)

    val b1 = serializer.serialize(topic, rf1.to(r1))
    assertEquals(rf1.from(deserializer.deserialize(topic, b1).asInstanceOf[GenericRecord]), r1)
    assertEquals(rf2.from(deserializer.deserialize(topic, b1).asInstanceOf[GenericRecord]), r2)

    val b2 = serializer.serialize(topic, rf2.to(r2))
    assertEquals(rf1.from(deserializer.deserialize(topic, b2).asInstanceOf[GenericRecord]), r1)
    assertEquals(rf2.from(deserializer.deserialize(topic, b2).asInstanceOf[GenericRecord]), r2)
  }

  test("Renaming a field is neither backward nor forward compatible") {
    val s1 = SchemaFor[RenameField1].schema(DefaultFieldMapper)
    val s2 = SchemaFor[RenameField2].schema(DefaultFieldMapper)
    assertEquals(Compatibility.backwardCompatible(s2, s1), false)
    assertEquals(Compatibility.forwardCompatible(s2, s1), false)

    val topic = testTopic
    val rf1 = RecordFormat[RenameField1]
    val rf2 = RecordFormat[RenameField2]
    val r1 = RenameField1(1)
    val r2 = RenameField2(1)

    val b1 = serializer.serialize(topic, rf1.to(r1))
    assertEquals(rf1.from(deserializer.deserialize(topic, b1).asInstanceOf[GenericRecord]), r1)
    assert(Try(rf2.from(
      deserializer.deserialize(topic, b1).asInstanceOf[GenericRecord]
    )).isFailure)

    serializer.serialize(topic, rf2.to(r2))
    val b2 = serializer.serialize(topic, rf2.to(r2))
    assert(Try(rf1.from(deserializer.deserialize(topic, b2).asInstanceOf[GenericRecord])).isFailure)
    assertEquals(rf2.from(deserializer.deserialize(topic, b2).asInstanceOf[GenericRecord]), r2)
  }

  test("Renaming the outer record is backward and forward compatible") {
    val s1 = SchemaFor[RenameOuterRecord1].schema(DefaultFieldMapper)
    val s2 = SchemaFor[RenameOuterRecord2].schema(DefaultFieldMapper)
    assertEquals(Compatibility.backwardCompatible(s2, s1), true)
    assertEquals(Compatibility.forwardCompatible(s2, s1), true)

    val topic = testTopic
    val rf1 = RecordFormat[RenameOuterRecord1]
    val rf2 = RecordFormat[RenameOuterRecord2]
    val r1 = RenameOuterRecord1(1)
    val r2 = RenameOuterRecord2(1)

    val b1 = serializer.serialize(topic, rf1.to(r1))
    assertEquals(rf1.from(deserializer.deserialize(topic, b1).asInstanceOf[GenericRecord]), r1)
    assertEquals(rf2.from(deserializer.deserialize(topic, b1).asInstanceOf[GenericRecord]), r2)

    val b2 = serializer.serialize(topic, rf2.to(r2))
    assertEquals(rf1.from(deserializer.deserialize(topic, b2).asInstanceOf[GenericRecord]), r1)
    assertEquals(rf2.from(deserializer.deserialize(topic, b2).asInstanceOf[GenericRecord]), r2)
  }

  test("Renaming an inner record is backward and forward compatible") {
    val s1 = SchemaFor[RenameInnerRecord1].schema(DefaultFieldMapper)
    val s2 = SchemaFor[RenameInnerRecord2].schema(DefaultFieldMapper)
    assertEquals(Compatibility.backwardCompatible(s2, s1), true)
    assertEquals(Compatibility.forwardCompatible(s2, s1), true)

    val topic = testTopic
    val rf1 = RecordFormat[RenameInnerRecord1]
    val rf2 = RecordFormat[RenameInnerRecord2]
    val r1 = RenameInnerRecord1(A("s"))
    val r2 = RenameInnerRecord2(A2("s"))

    val b1 = serializer.serialize(topic, rf1.to(r1))
    assertEquals(rf1.from(deserializer.deserialize(topic, b1).asInstanceOf[GenericRecord]), r1)
    assertEquals(rf2.from(deserializer.deserialize(topic, b1).asInstanceOf[GenericRecord]), r2)

    val b2 = serializer.serialize(topic, rf2.to(r2))
    assertEquals(rf1.from(deserializer.deserialize(topic, b2).asInstanceOf[GenericRecord]), r1)
    assertEquals(rf2.from(deserializer.deserialize(topic, b2).asInstanceOf[GenericRecord]), r2)
  }

  test("Adding a field without a default is forward, but not backward, compatible") {
    val s1 = SchemaFor[AddFieldWithoutDefault1].schema(DefaultFieldMapper)
    val s2 = SchemaFor[AddFieldWithoutDefault2].schema(DefaultFieldMapper)
    assertEquals(Compatibility.backwardCompatible(s2, s1), false)
    assertEquals(Compatibility.forwardCompatible(s2, s1), true)

    val topic = testTopic
    val rf1 = RecordFormat[AddFieldWithoutDefault1]
    val rf2 = RecordFormat[AddFieldWithoutDefault2]
    val r1 = AddFieldWithoutDefault1(1)
    val r2 = AddFieldWithoutDefault2(1, "s")

    val b1 = serializer.serialize(topic, rf1.to(r1))
    assertEquals(rf1.from(deserializer.deserialize(topic, b1).asInstanceOf[GenericRecord]), r1)
    assert(Try(rf2.from(
      deserializer.deserialize(topic, b1).asInstanceOf[GenericRecord]
    )).isFailure)

    val b2 = serializer.serialize(topic, rf2.to(r2))
    assertEquals(rf1.from(deserializer.deserialize(topic, b2).asInstanceOf[GenericRecord]), r1)
    assertEquals(rf2.from(deserializer.deserialize(topic, b2).asInstanceOf[GenericRecord]), r2)
  }

  test("Adding a field with a default is backward and forward compatible") {
    val s1 = SchemaFor[AddFieldWithDefault1].schema(DefaultFieldMapper)
    val s2 = SchemaFor[AddFieldWithDefault2].schema(DefaultFieldMapper)
    assertEquals(Compatibility.backwardCompatible(s2, s1), true)
    assertEquals(Compatibility.forwardCompatible(s2, s1), true)

    val topic = testTopic
    val rf1 = RecordFormat[AddFieldWithDefault1]
    val rf2 = RecordFormat[AddFieldWithDefault2]
    val r1 = AddFieldWithDefault1(1)
    val r2 = AddFieldWithDefault2(1, "s")

    val b1 = serializer.serialize(topic, rf1.to(r1))
    assertEquals(rf1.from(deserializer.deserialize(topic, b1).asInstanceOf[GenericRecord]), r1)
    assertEquals(rf2.from(deserializer.deserialize(topic, b1).asInstanceOf[GenericRecord]), r2)

    val b2 = serializer.serialize(topic, rf2.to(r2))
    assertEquals(rf1.from(deserializer.deserialize(topic, b2).asInstanceOf[GenericRecord]), r1)
    assertEquals(rf2.from(deserializer.deserialize(topic, b2).asInstanceOf[GenericRecord]), r2)
  }

  test("Removing a field without a default is backward, but not forward, compatible") {
    val s1 = SchemaFor[RemoveFieldWithoutDefault1].schema(DefaultFieldMapper)
    val s2 = SchemaFor[RemoveFieldWithoutDefault2].schema(DefaultFieldMapper)
    assertEquals(Compatibility.backwardCompatible(s2, s1), true)
    assertEquals(Compatibility.forwardCompatible(s2, s1), false)

    val topic = testTopic
    val rf1 = RecordFormat[RemoveFieldWithoutDefault1]
    val rf2 = RecordFormat[RemoveFieldWithoutDefault2]
    val r1 = RemoveFieldWithoutDefault1(1, "s")
    val r2 = RemoveFieldWithoutDefault2(1)

    val b1 = serializer.serialize(topic, rf1.to(r1))
    assertEquals(rf1.from(deserializer.deserialize(topic, b1).asInstanceOf[GenericRecord]), r1)
    assertEquals(rf2.from(deserializer.deserialize(topic, b1).asInstanceOf[GenericRecord]), r2)

    val b2 = serializer.serialize(topic, rf2.to(r2))
    assert(Try(rf1.from(
      deserializer.deserialize(topic, b2).asInstanceOf[GenericRecord]
    )).isFailure)
    assertEquals(rf2.from(deserializer.deserialize(topic, b2).asInstanceOf[GenericRecord]), r2)
  }

  test("Removing a field with a default is backward and forward compatible") {
    val s1 = SchemaFor[RemoveFieldWithDefault1].schema(DefaultFieldMapper)
    val s2 = SchemaFor[RemoveFieldWithDefault2].schema(DefaultFieldMapper)
    assertEquals(Compatibility.backwardCompatible(s2, s1), true)
    assertEquals(Compatibility.forwardCompatible(s2, s1), true)

    val topic = testTopic
    val rf1 = RecordFormat[RemoveFieldWithDefault1]
    val rf2 = RecordFormat[RemoveFieldWithDefault2]
    val r1 = RemoveFieldWithDefault1(1, "s")
    val r2 = RemoveFieldWithDefault2(1)

    val b1 = serializer.serialize(topic, rf1.to(r1))
    assertEquals(rf1.from(deserializer.deserialize(topic, b1).asInstanceOf[GenericRecord]), r1)
    assertEquals(rf2.from(deserializer.deserialize(topic, b1).asInstanceOf[GenericRecord]), r2)

    val b2 = serializer.serialize(topic, rf2.to(r2))
    assertEquals(rf1.from(deserializer.deserialize(topic, b2).asInstanceOf[GenericRecord]), r1)
    assertEquals(rf2.from(deserializer.deserialize(topic, b2).asInstanceOf[GenericRecord]), r2)
  }

  test("Reordering union types is backward and forward compatible") {
    val s1 = SchemaFor[ReorderUnionTypes1].schema(DefaultFieldMapper)
    val s2 = SchemaFor[ReorderUnionTypes2].schema(DefaultFieldMapper)
    assertEquals(Compatibility.backwardCompatible(s2, s1), true)
    assertEquals(Compatibility.forwardCompatible(s2, s1), true)

    val topic = testTopic
    val rf1 = RecordFormat[ReorderUnionTypes1]
    val rf2 = RecordFormat[ReorderUnionTypes2]
    val r1 = ReorderUnionTypes1(Left(A("s")))
    val r2 = ReorderUnionTypes2(Right(A("s")))

    val b1 = serializer.serialize(topic, rf1.to(r1))
    assertEquals(rf1.from(deserializer.deserialize(topic, b1).asInstanceOf[GenericRecord]), r1)
    assertEquals(rf2.from(deserializer.deserialize(topic, b1).asInstanceOf[GenericRecord]), r2)

    val b2 = serializer.serialize(topic, rf2.to(r2))
    assertEquals(rf1.from(deserializer.deserialize(topic, b2).asInstanceOf[GenericRecord]), r1)
    assertEquals(rf2.from(deserializer.deserialize(topic, b2).asInstanceOf[GenericRecord]), r2)
  }

  test("Adding a union type is backward, but not forward compatible") {
    val s1 = SchemaFor[AddUnionType1].schema(DefaultFieldMapper)
    val s2 = SchemaFor[AddUnionType2].schema(DefaultFieldMapper)
    assertEquals(Compatibility.backwardCompatible(s2, s1), true)
    assertEquals(Compatibility.forwardCompatible(s2, s1), false)

    val topic = testTopic
    val rf1 = RecordFormat[AddUnionType1]
    val rf2 = RecordFormat[AddUnionType2]
    val r1 = AddUnionType1(Left(A("s")))
    val r2 = AddUnionType2(Coproduct[ABC](A("s")))
    val r3 = AddUnionType2(Coproduct[ABC](C(true)))

    val b1 = serializer.serialize(topic, rf1.to(r1))
    assertEquals(rf1.from(deserializer.deserialize(topic, b1).asInstanceOf[GenericRecord]), r1)
    assertEquals(rf2.from(deserializer.deserialize(topic, b1).asInstanceOf[GenericRecord]), r2)

    //even though this change is not forward compatible, in practice a AddUnionType2 can be deserialized to AddUnionType1 as long as it doesn't contain the added type C
    val b2 = serializer.serialize(topic, rf2.to(r2))
    assertEquals(rf1.from(deserializer.deserialize(topic, b2).asInstanceOf[GenericRecord]), r1)
    assertEquals(rf2.from(deserializer.deserialize(topic, b2).asInstanceOf[GenericRecord]), r2)

    val b3 = serializer.serialize(topic, rf2.to(r3))
    assert(Try(rf1.from(
      deserializer.deserialize(topic, b3).asInstanceOf[GenericRecord]
    )).isFailure)
    assertEquals(rf2.from(deserializer.deserialize(topic, b3).asInstanceOf[GenericRecord]), r3)
  }

  test("Removing a union type is forward, but not backward compatible") {
    val s1 = SchemaFor[RemoveUnionType1].schema(DefaultFieldMapper)
    val s2 = SchemaFor[RemoveUnionType2].schema(DefaultFieldMapper)
    assertEquals(Compatibility.backwardCompatible(s2, s1), false)
    assertEquals(Compatibility.forwardCompatible(s2, s1), true)

    val topic = testTopic
    val rf1 = RecordFormat[RemoveUnionType1]
    val rf2 = RecordFormat[RemoveUnionType2]
    val r1 = RemoveUnionType1(Coproduct[ABC](A("s")))
    val r2 = RemoveUnionType2(Left(A("s")))
    val r3 = RemoveUnionType1(Coproduct[ABC](C(true)))

    //even though this change is not backward compatible, in practice a AddUnionType1 can be deserialized to AddUnionType2 as long as it doesn't contain the removed type C
    val b1 = serializer.serialize(topic, rf1.to(r1))
    assertEquals(rf1.from(deserializer.deserialize(topic, b1).asInstanceOf[GenericRecord]), r1)
    assertEquals(rf2.from(deserializer.deserialize(topic, b1).asInstanceOf[GenericRecord]), r2)

    val b2 = serializer.serialize(topic, rf2.to(r2))
    assertEquals(rf1.from(deserializer.deserialize(topic, b2).asInstanceOf[GenericRecord]), r1)
    assertEquals(rf2.from(deserializer.deserialize(topic, b2).asInstanceOf[GenericRecord]), r2)

    val b3 = serializer.serialize(topic, rf1.to(r3))
    assertEquals(rf1.from(deserializer.deserialize(topic, b3).asInstanceOf[GenericRecord]), r3)
    assert(Try(rf2.from(
      deserializer.deserialize(topic, b3).asInstanceOf[GenericRecord]
    )).isFailure)
  }

}
