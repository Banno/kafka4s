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

package com.banno.kafka.connect

import cats.effect.Sync
import org.apache.kafka.connect.source.SourceTaskContext
import scala.collection.JavaConverters._

trait SourceTaskContextApi[F[_], P, O, TC <: TaskConfigs[P, O]] {
  def configs: F[TC]
  def offset(partition: P): F[Option[O]]
  def offsets(partitions: Iterable[P]): F[Map[P, O]]
}

object SourceTaskContextApi {
  def apply[F[_]: Sync, P: MapEncoder: MapDecoder, O: MapDecoder, TC <: TaskConfigs[P, O]: MapDecoder](
      c: SourceTaskContext
  ): SourceTaskContextApi[F, P, O, TC] = new SourceTaskContextApi[F, P, O, TC] {
    def configs: F[TC] = Sync[F].delay(MapDecoder[TC].decode(c.configs().asScala.toMap))
    def offset(partition: P): F[Option[O]] =
      Sync[F].delay(
        Option(c.offsetStorageReader.offset[String](MapEncoder[P].encode(partition).asJava))
          .map(m => MapDecoder[O].decode(m.asScala.toMap.mapValues(_.toString)))
      )
    def offsets(partitions: Iterable[P]): F[Map[P, O]] =
      Sync[F].delay(
        c.offsetStorageReader
          .offsets[String](partitions.map(p => MapEncoder[P].encode(p).asJava).asJavaCollection)
          .asScala
          .toMap
          .filter(_._2 != null)
          .map {
            case (pm, om) =>
              (
                MapDecoder[P].decode(pm.asScala.toMap),
                MapDecoder[O].decode(om.asScala.toMap.mapValues(_.toString))
              )
          }
      )
  }
}
