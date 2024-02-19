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

package com.banno.kafka.publish

import cats.*
import cats.syntax.all.*
import com.banno.kafka.Topical
import com.banno.kafka.producer.ProducerApi
import fs2.Pipe
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.producer.RecordMetadata

object PublishStream {
  type T[F[_], A] = Pipe[F, A, F[RecordMetadata]]
  type KV[F[_], K, V] = T[F, (K, V)]

  /** Makes a stream capable of publishing records in batches or sequentially.
    * Batch size will depend on the size of the underlying chunks in the stream.
    */

  def to[F[_]: MonadThrow, A, B](
      topical: Topical[A, B],
      producer: ProducerApi[F, GenericRecord, GenericRecord],
  ): T[F, B] = kv =>
    kv
      .evalMapChunk(b => topical.coparse(b).liftTo[F])
      .evalMapChunk(r => producer.send(r))

}
