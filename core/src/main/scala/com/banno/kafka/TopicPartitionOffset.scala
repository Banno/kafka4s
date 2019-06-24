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

import org.apache.kafka.common.TopicPartition
import org.apache.kafka.clients.consumer.OffsetAndMetadata

case class TopicPartitionOffset(topic: String, partition: Int, offset: Long)
object TopicPartitionOffset {
  implicit def toMap(o: TopicPartitionOffset): Map[TopicPartition, OffsetAndMetadata] = 
    Map(new TopicPartition(o.topic, o.partition) -> new OffsetAndMetadata(o.offset))
}
