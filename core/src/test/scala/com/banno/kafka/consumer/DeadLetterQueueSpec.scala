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

import munit.FunSuite
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.header.internals.RecordHeaders
import DeadLetterQueue._

class DeadLetterQueueSpec extends FunSuite {

  test("convert failed ConsumerRecord into DeadLetter ProducerRecord with metadata headers") {
    val headers = new RecordHeaders()
    val cr = new ConsumerRecord[String, String](
      "banking-transactions",
      2,
      4200L,
      1700000000000L,
      org.apache.kafka.common.record.TimestampType.CREATE_TIME,
      0,
      0,
      "account-123",
      "{\"amount\": 500}",
      headers,
      java.util.Optional.empty()
    )

    val error = new IllegalArgumentException("Invalid currency format in payload")
    val dlqRecord = cr.toDeadLetter("banking-transactions.dlq", error, timestamp = 1700000005000L)

    assertEquals(dlqRecord.topic(), "banking-transactions.dlq")
    assertEquals(dlqRecord.key(), "account-123")
    assertEquals(dlqRecord.value(), "{\"amount\": 500}")
    assertEquals(dlqRecord.timestamp().toLong, 1700000005000L)

    val meta = DeadLetterQueue.extractDeadLetterMetadata(dlqRecord.headers())
    assert(meta.isDefined)
    val m = meta.get
    assertEquals(m.originalTopic, "banking-transactions")
    assertEquals(m.originalPartition, 2)
    assertEquals(m.originalOffset, 4200L)
    assertEquals(m.exceptionClass, "java.lang.IllegalArgumentException")
    assertEquals(m.exceptionMessage, "Invalid currency format in payload")
    assertEquals(m.timestamp, 1700000005000L)
  }
}
