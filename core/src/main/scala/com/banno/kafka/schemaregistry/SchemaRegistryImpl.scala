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

package com.banno.kafka.schemaregistry

import scala.collection.compat._
import org.apache.avro.Schema
import io.confluent.kafka.schemaregistry.client.{SchemaMetadata, SchemaRegistryClient}
import cats.effect.Sync
import scala.jdk.CollectionConverters._

case class SchemaRegistryImpl[F[_]](c: SchemaRegistryClient)(implicit F: Sync[F])
    extends SchemaRegistryApi[F] {
  import SchemaRegistryApi._

  def getAllSubjects: F[Iterable[String]] = F.delay(c.getAllSubjects().asScala)
  def getById(id: Int): F[Schema] = F.delay(c.getById(id))
  def getBySubjectAndId(subject: String, id: Int): F[Schema] =
    F.delay(c.getBySubjectAndId(subject, id))
  def getCompatibility(subject: String): F[SchemaRegistryApi.CompatibilityLevel] =
    F.delay(CompatibilityLevel.unsafeFromString(c.getCompatibility(subject)))
  def getLatestSchemaMetadata(subject: String): F[SchemaMetadata] =
    F.delay(c.getLatestSchemaMetadata(subject))
  def getSchemaMetadata(subject: String, version: Int): F[SchemaMetadata] =
    F.delay(c.getSchemaMetadata(subject, version))
  def getVersion(subject: String, schema: Schema): F[Int] = F.delay(c.getVersion(subject, schema))
  def register(subject: String, schema: Schema): F[Int] = F.delay(c.register(subject, schema))
  def testCompatibility(subject: String, schema: Schema): F[Boolean] =
    F.delay(c.testCompatibility(subject, schema))
  def updateCompatibility(subject: String, compatibility: CompatibilityLevel): F[String] =
    F.delay(c.updateCompatibility(subject, compatibility.asString))
}
