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

package com.banno.kafka.avro4s

import cats.*
import cats.effect.*
import cats.syntax.all.*
import com.banno.kafka.consumer.*
import com.sksamuel.avro4s.FromRecord
import org.apache.avro.generic.GenericRecord

object Avro4sConsumer {
  def apply[F[_]: Functor, K, V](
      c: ConsumerApi[F, GenericRecord, GenericRecord]
  )(implicit
      kfr: FromRecord[K],
      vfr: FromRecord[V],
  ): ConsumerApi[F, K, V] =
    c.bimap(kfr.from, vfr.from)

  def resource[F[_]: Async, K: FromRecord, V: FromRecord](
      configs: (String, AnyRef)*
  ): Resource[F, ConsumerApi[F, K, V]] =
    ConsumerApi.Avro.Generic.resource[F](configs: _*).map(Avro4sConsumer(_))
}
