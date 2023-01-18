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

import cats.effect.*
import com.banno.kafka.producer.*
import org.apache.avro.generic.GenericRecord

object VulcanProducer {
  def apply[F[_], K, V](
      p: ProducerApi[F, GenericRecord, GenericRecord]
  ): ProducerApi[F, K, V] =
    p.contrabimap(_ => ???, _ => ???)

  def resource[F[_], K, V](
      configs: (String, AnyRef)*
  ): Resource[F, ProducerApi[F, K, V]] = {
    val _ = configs
    ???
  }
}
