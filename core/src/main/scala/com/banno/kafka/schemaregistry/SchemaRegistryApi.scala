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

import org.apache.avro.Schema
import io.confluent.kafka.schemaregistry.client.{CachedSchemaRegistryClient, SchemaMetadata}
import io.confluent.kafka.schemaregistry.client.rest.RestService
import cats.implicits._
import cats.effect.Sync
import com.sksamuel.avro4s.SchemaFor
import org.typelevel.log4cats.slf4j.Slf4jLogger
import io.confluent.kafka.schemaregistry.ParsedSchema

import scala.jdk.CollectionConverters._
import scala.util.control.NoStackTrace

trait SchemaRegistryApi[F[_]] {
  import SchemaRegistryApi._

  def getAllSubjects: F[Iterable[String]]

  @deprecated("Use getSchemaById instead.", "3.0.0-M24")
  def getById(id: Int): F[Schema]

  @deprecated("Use getSchemaBySubjectAndId instead.", "3.0.0-M24")
  def getSchemaById(id: Int): F[ParsedSchema]

  @deprecated("Use getSchemaBySubjectAndId instead.", "3.0.0-M24")
  def getBySubjectAndId(subject: String, id: Int): F[Schema]

  def getSchemaBySubjectAndId(subject: String, id: Int): F[ParsedSchema]
  def getCompatibility(subject: String): F[CompatibilityLevel]
  def getLatestSchemaMetadata(subject: String): F[SchemaMetadata]
  def getSchemaMetadata(subject: String, version: Int): F[SchemaMetadata]

  @deprecated("Use getVersion(String,ParsedSchema) instead.", "3.0.0-M24")
  def getVersion(subject: String, schema: Schema): F[Int]

  def getVersion(subject: String, schema: ParsedSchema): F[Int]

  @deprecated("Use register(String,ParsedSchema) instead.", "3.0.0-M24")
  def register(subject: String, schema: Schema): F[Int]

  def register(subject: String, schema: ParsedSchema): F[Int]

  @deprecated("Use testCompatibility(String,ParsedSchema) instead.", "3.0.0-M24")
  def testCompatibility(subject: String, schema: Schema): F[Boolean]

  def testCompatibility(subject: String, schema: ParsedSchema): F[Boolean]

  def updateCompatibility(subject: String, compatibility: CompatibilityLevel): F[String]
}

object SchemaRegistryApi {
  def log[F[_]: Sync] = Slf4jLogger.getLoggerFromClass(this.getClass)

  def createClient[F[_]: Sync](
      baseUrl: String,
      identityMapCapacity: Int,
      configs: Map[String, Object] = Map.empty,
  ): F[CachedSchemaRegistryClient] =
    Sync[F].delay(new CachedSchemaRegistryClient(baseUrl, identityMapCapacity, configs.asJava))
  def createClient[F[_]: Sync](
      baseUrls: Seq[String],
      identityMapCapacity: Int
  ): F[CachedSchemaRegistryClient] =
    Sync[F].delay(new CachedSchemaRegistryClient(baseUrls.asJava, identityMapCapacity))
  def createClient[F[_]: Sync](
      restService: RestService,
      identityMapCapacity: Int
  ): F[CachedSchemaRegistryClient] =
    Sync[F].delay(new CachedSchemaRegistryClient(restService, identityMapCapacity))

  def apply[F[_]: Sync](
    baseUrl: String,
    configs: Map[String, Object] = Map.empty,
  ): F[SchemaRegistryApi[F]] =
    createClient[F](baseUrl, identityMapCapacity = 1024, configs = configs).map(SchemaRegistryImpl[F](_))
  def apply[F[_]: Sync](baseUrl: String, identityMapCapacity: Int): F[SchemaRegistryApi[F]] =
    createClient[F](baseUrl, identityMapCapacity).map(SchemaRegistryImpl[F](_))
  def apply[F[_]: Sync](baseUrls: Seq[String], identityMapCapacity: Int): F[SchemaRegistryApi[F]] =
    createClient[F](baseUrls, identityMapCapacity).map(SchemaRegistryImpl[F](_))
  def apply[F[_]: Sync](
      restService: RestService,
      identityMapCapacity: Int
  ): F[SchemaRegistryApi[F]] =
    createClient[F](restService, identityMapCapacity).map(SchemaRegistryImpl[F](_))

  def register[F[_]: Sync, K: SchemaFor, V: SchemaFor](
    baseUrl: String,
    topic: String,
    configs: Map[String, Object] = Map.empty,
  ) =
    for {
      schemaRegistry <- apply(baseUrl, configs)
      k <- schemaRegistry.registerKey[K](topic)
      _ <- log.debug(s"Registered key schema for topic ${topic} at ${baseUrl}")
      v <- schemaRegistry.registerValue[V](topic)
      _ <- log.debug(s"Registered value schema for topic ${topic} at ${baseUrl}")
    } yield (k, v)

  sealed trait CompatibilityLevel {
    def asString: String
  }

  object CompatibilityLevel {
    case class ParseFailure(message: String) extends RuntimeException with NoStackTrace

    case object Backward extends CompatibilityLevel {
      val asString = "BACKWARD"
    }

    case object BackwardTransitive extends CompatibilityLevel {
      val asString = "BACKWARD_TRANSITIVE"
    }

    case object Forward extends CompatibilityLevel {
      val asString = "FORWARD"
    }

    case object ForwardTransitive extends CompatibilityLevel {
      val asString = "FORWARD_TRANSITIVE"
    }

    case object Full extends CompatibilityLevel {
      val asString = "FULL"
    }

    case object FullTransitive extends CompatibilityLevel {
      val asString = "FULL_TRANSITIVE"
    }

    case object None extends CompatibilityLevel {
      val asString = "NONE"
    }

    def fromString(s: String): Option[CompatibilityLevel] = s match {
      case s if s === Backward.asString => Backward.some
      case s if s === BackwardTransitive.asString => BackwardTransitive.some
      case s if s === Forward.asString => Forward.some
      case s if s === ForwardTransitive.asString => ForwardTransitive.some
      case s if s === Full.asString => Full.some
      case s if s === FullTransitive.asString => FullTransitive.some
      case s if s === None.asString => None.some
      case _ => none
    }

    def unsafeFromString(s: String): CompatibilityLevel =
      fromString(s).getOrElse(throw ParseFailure(s"Unable to parse CompatibilityLevel: $s"))
  }
}
