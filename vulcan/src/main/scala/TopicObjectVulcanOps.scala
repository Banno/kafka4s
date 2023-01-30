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
package vulcan

import _root_.vulcan.*
import cats.*
import cats.syntax.all.*

object TopicObjectVulcanOps {
  def apply[F[_]: ApplicativeThrow, K: Codec, V: Codec](
      topic: String,
      topicPurpose: TopicPurpose,
  ): F[Topic[K, V]] =
    (Schema.vulcan[F, K], Schema.vulcan[F, V]).mapN(
      Topic(
        topic,
        topicPurpose,
        _,
        _,
      )
    )

  def builder[F[_]: ApplicativeThrow, K: Codec, V: Codec](
      topic: String,
      topicPurpose: TopicPurpose,
  ): F[Topic.Builder[K, V]] =
    (Schema.vulcan[F, K], Schema.vulcan[F, V]).mapN(
      Topic.builder(
        topic,
        topicPurpose,
        _,
        _,
      )
    )
}
