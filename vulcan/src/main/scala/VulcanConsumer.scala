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

package com.banno.kafka.vulcan

import cats.*
import cats.effect.*
import com.banno.kafka.consumer.*
import org.apache.avro.generic.GenericRecord
import vulcan.*

object VulcanConsumer {
  def apply[F[_]: MonadThrow, K: Codec, V: Codec](
      c: ConsumerApi[F, GenericRecord, GenericRecord]
  ): ConsumerApi[F, K, V] =
    c.biSemiflatMap(
      Codec.decodeGenericRecord[F, K](_),
      Codec.decodeGenericRecord[F, V](_),
    )

  def resource[F[_]: Async, K: Codec, V: Codec](
      configs: (String, AnyRef)*
  ): Resource[F, ConsumerApi[F, K, V]] =
    ConsumerApi.Avro.Generic.resource[F](configs: _*).map(apply(_))
}
