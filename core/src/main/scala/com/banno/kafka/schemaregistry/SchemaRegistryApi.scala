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
package schemaregistry

import cats.*
import cats.effect.*
import cats.syntax.all.*
import io.confluent.kafka.schemaregistry.ParsedSchema
import io.confluent.kafka.schemaregistry.client.rest.RestService
import io.confluent.kafka.schemaregistry.client.{
  CachedSchemaRegistryClient,
  SchemaMetadata,
}
import org.apache.avro.{Schema as JSchema}
import org.typelevel.log4cats.slf4j.Slf4jLogger
import scala.jdk.CollectionConverters.*
import scala.util.control.NoStackTrace

trait SchemaRegistryApi[F[_]] {
  import SchemaRegistryApi._

  def getAllSubjects: F[Iterable[String]]

  @deprecated("Use getSchemaById instead.", "3.0.0-M24")
  def getById(id: Int): F[JSchema]

  @deprecated("Use getSchemaBySubjectAndId instead.", "3.0.0-M24")
  def getSchemaById(id: Int): F[ParsedSchema]

  @deprecated("Use getSchemaBySubjectAndId instead.", "3.0.0-M24")
  def getBySubjectAndId(subject: String, id: Int): F[JSchema]

  def getSchemaBySubjectAndId(subject: String, id: Int): F[ParsedSchema]
  def getCompatibility(subject: String): F[CompatibilityLevel]
  def getLatestSchemaMetadata(subject: String): F[SchemaMetadata]
  def getSchemaMetadata(subject: String, version: Int): F[SchemaMetadata]

  @deprecated("Use getVersion(String,ParsedSchema) instead.", "3.0.0-M24")
  def getVersion(subject: String, schema: JSchema): F[Int]

  def getVersion(subject: String, schema: ParsedSchema): F[Int]

  @deprecated("Use register(String,ParsedSchema) instead.", "3.0.0-M24")
  def register(subject: String, schema: JSchema): F[Int]

  def register(subject: String, schema: ParsedSchema): F[Int]

  @deprecated(
    "Use testCompatibility(String,ParsedSchema) instead.",
    "3.0.0-M24",
  )
  def testCompatibility(subject: String, schema: JSchema): F[Boolean]

  def testCompatibility(subject: String, schema: ParsedSchema): F[Boolean]

  def updateCompatibility(
      subject: String,
      compatibility: CompatibilityLevel,
  ): F[String]

  final def keySubject(topic: String): String = topic + "-key"
  final def valueSubject(topic: String): String = topic + "-value"

  final def register[A](subject: String, schema: Schema[A]): F[Int] =
    register(subject, schema.parsed)

  final def registerKey[K](topic: String, schema: Schema[K]): F[Int] =
    register[K](keySubject(topic), schema)

  final def registerValue[V](topic: String, schema: Schema[V]): F[Int] =
    register[V](valueSubject(topic), schema)

  final def register[K, V](
      topic: String,
      keySchema: Schema[K],
      valueSchema: Schema[V],
  )(implicit F: FlatMap[F]): F[(Int, Int)] =
    for {
      k <- registerKey(topic, keySchema)
      v <- registerValue(topic, valueSchema)
    } yield (k, v)

  final def isCompatible(subject: String, schema: JSchema): F[Boolean] =
    testCompatibility(subject, schema.asParsedSchema)

  final def isCompatible[A](subject: String, schema: Schema[A]): F[Boolean] =
    isCompatible(subject, schema.ast)

  final def isKeyCompatible[K](topic: String, schema: Schema[K]): F[Boolean] =
    isCompatible(keySubject(topic), schema)

  final def isValueCompatible[V](topic: String, schema: Schema[V]): F[Boolean] =
    isCompatible(valueSubject(topic), schema)

  final def isCompatible[K, V](
      topic: String,
      keySchema: Schema[K],
      valueSchema: Schema[V],
  )(implicit F: FlatMap[F]): F[(Boolean, Boolean)] =
    for {
      k <- isKeyCompatible[K](topic, keySchema)
      v <- isValueCompatible[V](topic, valueSchema)
    } yield (k, v)
}

object SchemaRegistryApi {
  def log[F[_]: Sync] = Slf4jLogger.getLoggerFromClass(this.getClass)

  def createClient[F[_]: Sync](
      baseUrl: String,
      identityMapCapacity: Int,
      configs: Map[String, Object] = Map.empty,
  ): F[CachedSchemaRegistryClient] =
    Sync[F].delay(
      new CachedSchemaRegistryClient(
        baseUrl,
        identityMapCapacity,
        configs.asJava,
      )
    )
  def createClient[F[_]: Sync](
      baseUrls: Seq[String],
      identityMapCapacity: Int,
  ): F[CachedSchemaRegistryClient] =
    Sync[F].delay(
      new CachedSchemaRegistryClient(baseUrls.asJava, identityMapCapacity)
    )
  def createClient[F[_]: Sync](
      restService: RestService,
      identityMapCapacity: Int,
  ): F[CachedSchemaRegistryClient] =
    Sync[F].delay(
      new CachedSchemaRegistryClient(restService, identityMapCapacity)
    )

  def apply[F[_]: Sync](
      baseUrl: String,
      configs: Map[String, Object] = Map.empty,
  ): F[SchemaRegistryApi[F]] =
    createClient[F](baseUrl, identityMapCapacity = 1024, configs = configs)
      .map(SchemaRegistryImpl[F](_))
  def apply[F[_]: Sync](
      baseUrl: String,
      identityMapCapacity: Int,
  ): F[SchemaRegistryApi[F]] =
    createClient[F](baseUrl, identityMapCapacity).map(SchemaRegistryImpl[F](_))
  def apply[F[_]: Sync](
      baseUrls: Seq[String],
      identityMapCapacity: Int,
  ): F[SchemaRegistryApi[F]] =
    createClient[F](baseUrls, identityMapCapacity).map(SchemaRegistryImpl[F](_))
  def apply[F[_]: Sync](
      restService: RestService,
      identityMapCapacity: Int,
  ): F[SchemaRegistryApi[F]] =
    createClient[F](restService, identityMapCapacity).map(
      SchemaRegistryImpl[F](_)
    )

  sealed trait CompatibilityLevel {
    def asString: String
  }

  object CompatibilityLevel {
    case class ParseFailure(message: String)
        extends RuntimeException
        with NoStackTrace

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
      fromString(s).getOrElse(
        throw ParseFailure(s"Unable to parse CompatibilityLevel: $s")
      )

    val values: List[CompatibilityLevel] =
      List(
        Backward,
        BackwardTransitive,
        Forward,
        ForwardTransitive,
        Full,
        FullTransitive,
        None,
      )
  }

  def register[F[_]: Sync, K, V](
      baseUrl: String,
      topic: String,
      keySchema: Schema[K],
      valueSchema: Schema[V],
      configs: Map[String, Object] = Map.empty,
  ) =
    for {
      schemaRegistry <- SchemaRegistryApi(baseUrl, configs)
      k <- schemaRegistry.registerKey[K](topic, keySchema)
      v <- schemaRegistry.registerValue[V](topic, valueSchema)
    } yield (k, v)
}
