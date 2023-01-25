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

import com.banno.kafka.schemaregistry.*

package object vulcan {
  implicit final class SchemaRegistryVulcanOpsOps[F[_]](
      private val r: SchemaRegistryApi[F]
  ) extends AnyVal {
    def vulcan: SchemaRegistryVulcanOps[F] =
      new SchemaRegistryVulcanOps(r)
  }

  implicit final class SchemaRegistryObjectVulcanOpsOps(
      private val x: SchemaRegistryApi.type
  ) extends AnyVal {
    def vulcan = SchemaRegistryApiObjectVulcanOps
  }

  implicit final class TopicObjectVulcanOpsOps(
      private val x: Topic.type
  ) extends AnyVal {
    def vulcan = TopicObjectVulcanOps
  }

  implicit final class SchemaObjectVulcanOpsOps(
      private val x: Schema.type
  ) extends AnyVal {
    def vulcan = SchemaObjectVulcanOps
  }
}
