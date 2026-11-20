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

package com.banno.kafka.consumer

import java.nio.charset.StandardCharsets
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.header.Headers
import org.apache.kafka.common.header.internals.RecordHeader
import scala.jdk.CollectionConverters._

/** Metadata captured when a record fails processing and is routed to a Dead-Letter Queue.
  */
final case class DeadLetterMetadata(
    originalTopic: String,
    originalPartition: Int,
    originalOffset: Long,
    exceptionClass: String,
    exceptionMessage: String,
    timestamp: Long
)

object DeadLetterQueue {
  val OriginalTopicHeader = "x-dlq-original-topic"
  val OriginalPartitionHeader = "x-dlq-original-partition"
  val OriginalOffsetHeader = "x-dlq-original-offset"
  val ExceptionClassHeader = "x-dlq-exception-class"
  val ExceptionMessageHeader = "x-dlq-exception-message"
  val TimestampHeader = "x-dlq-timestamp"

  /** Converts a failed ConsumerRecord into a ProducerRecord targeting the DLQ topic,
    * preserving key, value, and existing headers while appending diagnostic DLQ error headers.
    */
  def toDeadLetterRecord[K, V](
      record: ConsumerRecord[K, V],
      dlqTopic: String,
      error: Throwable,
      timestamp: Long = System.currentTimeMillis()
  ): ProducerRecord[K, V] = {
    val pr = new ProducerRecord[K, V](
      dlqTopic,
      null, // let partitioner choose or preserve null
      timestamp,
      record.key(),
      record.value(),
      record.headers()
    )

    attachDeadLetterHeaders(
      pr.headers(),
      DeadLetterMetadata(
        originalTopic = record.topic(),
        originalPartition = record.partition(),
        originalOffset = record.offset(),
        exceptionClass = error.getClass.getName,
        exceptionMessage = Option(error.getMessage).getOrElse(""),
        timestamp = timestamp
      )
    )

    pr
  }

  /** Appends DLQ error headers to an existing Kafka Headers collection. */
  def attachDeadLetterHeaders(headers: Headers, meta: DeadLetterMetadata): Headers = {
    headers.add(new RecordHeader(OriginalTopicHeader, meta.originalTopic.getBytes(StandardCharsets.UTF_8)))
    headers.add(new RecordHeader(OriginalPartitionHeader, meta.originalPartition.toString.getBytes(StandardCharsets.UTF_8)))
    headers.add(new RecordHeader(OriginalOffsetHeader, meta.originalOffset.toString.getBytes(StandardCharsets.UTF_8)))
    headers.add(new RecordHeader(ExceptionClassHeader, meta.exceptionClass.getBytes(StandardCharsets.UTF_8)))
    headers.add(new RecordHeader(ExceptionMessageHeader, meta.exceptionMessage.getBytes(StandardCharsets.UTF_8)))
    headers.add(new RecordHeader(TimestampHeader, meta.timestamp.toString.getBytes(StandardCharsets.UTF_8)))
    headers
  }

  /** Extracts DeadLetterMetadata from Kafka Headers if present. */
  def extractDeadLetterMetadata(headers: Headers): Option[DeadLetterMetadata] = {
    for {
      topicH <- Option(headers.lastHeader(OriginalTopicHeader))
      partH <- Option(headers.lastHeader(OriginalPartitionHeader))
      offsetH <- Option(headers.lastHeader(OriginalOffsetHeader))
      excClassH <- Option(headers.lastHeader(ExceptionClassHeader))
      excMsgH <- Option(headers.lastHeader(ExceptionMessageHeader))
      tsH <- Option(headers.lastHeader(TimestampHeader))
      part <- Option(partH.value()).flatMap(b => new String(b, StandardCharsets.UTF_8).toIntOption)
      offset <- Option(offsetH.value()).flatMap(b => new String(b, StandardCharsets.UTF_8).toLongOption)
      ts <- Option(tsH.value()).flatMap(b => new String(b, StandardCharsets.UTF_8).toLongOption)
    } yield DeadLetterMetadata(
      originalTopic = new String(topicH.value(), StandardCharsets.UTF_8),
      originalPartition = part,
      originalOffset = offset,
      exceptionClass = new String(excClassH.value(), StandardCharsets.UTF_8),
      exceptionMessage = new String(excMsgH.value(), StandardCharsets.UTF_8),
      timestamp = ts
    )
  }

  implicit class ConsumerRecordDeadLetterOps[K, V](cr: ConsumerRecord[K, V]) {
    def toDeadLetter(dlqTopic: String, error: Throwable, timestamp: Long = System.currentTimeMillis()): ProducerRecord[K, V] =
      toDeadLetterRecord(cr, dlqTopic, error, timestamp)

    def deadLetterMetadata: Option[DeadLetterMetadata] =
      extractDeadLetterMetadata(cr.headers())
  }
}
