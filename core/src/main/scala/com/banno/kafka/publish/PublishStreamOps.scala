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

import cats.Applicative
import cats.effect.Concurrent
import fs2.Stream

final class PublishStreamOps[F[_], A](private val stream: Stream[F, F[A]])
    extends AnyVal {

  def batched(implicit A: Applicative[F]): Stream[F, A] =
    stream.evalMapChunk(identity)

  def parBatched(maxConcurrent: Int)(implicit C: Concurrent[F]): Stream[F, A] =
    if (maxConcurrent > 0) stream.parEvalMap(maxConcurrent)(identity)
    else stream.parEvalMapUnbounded(identity)

  def parBatchedUnbounded(implicit C: Concurrent[F]): Stream[F, A] =
    stream.parEvalMapUnbounded(identity)

}
