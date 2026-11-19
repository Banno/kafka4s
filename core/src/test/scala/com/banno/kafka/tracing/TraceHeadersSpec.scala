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

package com.banno.kafka.tracing

import munit.FunSuite
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.header.internals.RecordHeaders
import TraceHeaders._

class TraceHeadersSpec extends FunSuite {

  val sampleTraceparent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"

  test("parse valid W3C traceparent string") {
    val parsed = W3CTraceparent.parse(sampleTraceparent)
    assert(parsed.isDefined)
    val tp = parsed.get
    assertEquals(tp.version, "00")
    assertEquals(tp.traceId, "4bf92f3577b34da6a3ce929d0e0e4736")
    assertEquals(tp.parentId, "00f067aa0ba902b7")
    assertEquals(tp.traceFlags, "01")
    assertEquals(tp.asString, sampleTraceparent)
  }

  test("reject invalid W3C traceparent string") {
    assertEquals(W3CTraceparent.parse(null), None)
    assertEquals(W3CTraceparent.parse("invalid"), None)
    assertEquals(W3CTraceparent.parse("00-short-traceid-01"), None)
  }

  test("inject and extract traceparent from ProducerRecord") {
    val pr = new ProducerRecord[String, String]("my-topic", "key1", "val1")
    assertEquals(pr.traceparent, None)

    pr.withTraceparent(sampleTraceparent)
    val extracted = pr.traceparent
    assert(extracted.isDefined)
    assertEquals(extracted.get.asString, sampleTraceparent)
  }

  test("inject and extract tracestate from ProducerRecord") {
    val pr = new ProducerRecord[String, String]("my-topic", "key1", "val1")
    pr.withTracestate("congo=t61rcWkgMzE,rojo=00f067aa0ba902b7")
    assertEquals(pr.tracestate, Some("congo=t61rcWkgMzE,rojo=00f067aa0ba902b7"))
  }

  test("extract traceparent and tracestate from ConsumerRecord") {
    val headers = new RecordHeaders()
    TraceHeaders.injectTraceparent(
      headers,
      W3CTraceparent("00", "4bf92f3577b34da6a3ce929d0e0e4736", "00f067aa0ba902b7", "01")
    )
    TraceHeaders.injectTracestate(headers, "vendor1=opaqueValue")

    val cr = new ConsumerRecord[String, String](
      "my-topic",
      0,
      100L,
      0L,
      org.apache.kafka.common.record.TimestampType.CREATE_TIME,
      0,
      0,
      "key1",
      "val1",
      headers,
      java.util.Optional.empty()
    )

    val tp = cr.traceparent
    assert(tp.isDefined)
    assertEquals(tp.get.asString, sampleTraceparent)
    assertEquals(cr.tracestate, Some("vendor1=opaqueValue"))
  }
}
