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

package com.banno.kafka.admin

import cats.syntax.all._
import cats.effect.Sync
import org.apache.kafka.clients.admin._
import scala.jdk.CollectionConverters._

case class AdminOps[F[_]](admin: AdminApi[F]) {

  /** Only creates the specified topics that do not already exist. */
  def createTopicsIdempotent(
      newTopics: Iterable[NewTopic]
  )(implicit F: Sync[F]): F[CreateTopicsResult] =
    for {
      ltr <- admin.listTopics
      ns <- F.delay(ltr.names().get())
      ctr <- admin.createTopics(newTopics.filterNot(t => ns.contains(t.name)))
    } yield ctr

  /** Attempts to create all specified topics, and returns the result for each topic name. 
   * Right(()) means the topic was created successfully.
   * Note that if the topic already exists, the result will be Left(TopicExistsException), which makes this operation idempotent for each topic. */
  def createTopics(newTopics: NewTopic*)(implicit F: Sync[F]): F[Map[String, Either[Throwable, Unit]]] = 
    for {
      result <- admin.createTopics(newTopics)
      allResults <- result.values().asScala.toList.traverse{case (topic, future) => Sync[F].blocking(future.get()).attempt.map((topic, _))}
    } yield allResults.map{case (t, e) => (t, e.map(_ => ()))}.toMap

}
