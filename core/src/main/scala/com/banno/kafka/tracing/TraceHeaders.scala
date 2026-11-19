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

import java.nio.charset.StandardCharsets
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.header.Headers
import org.apache.kafka.common.header.internals.RecordHeader
import scala.jdk.CollectionConverters._

/** W3C Trace Context representation compliant with the W3C Trace Context specification.
  * https://www.w3.org/TR/trace-context/
  */
final case class W3CTraceparent(
    version: String,
    traceId: String,
    parentId: String,
    traceFlags: String
) {
  /** Formats the traceparent into standard W3C 4-part string format:
    * `${version}-${traceId}-${parentId}-${traceFlags}`
    */
  def asString: String = s"$version-$traceId-$parentId-$traceFlags"

  def asBytes: Array[Byte] = asString.getBytes(StandardCharsets.UTF_8)
}

object W3CTraceparent {
  val HeaderName = "traceparent"

  /** Parses a W3C traceparent string into a W3CTraceparent case class.
    * Returns None if the string does not match the 4-part hex specification.
    */
  def parse(raw: String): Option[W3CTraceparent] = {
    if (raw == null) None
    else {
      val parts = raw.trim.split("-")
      if (parts.length == 4 && parts(0).length == 2 && parts(1).length == 32 && parts(2).length == 16 && parts(3).length == 2) {
        Some(W3CTraceparent(parts(0), parts(1), parts(2), parts(3)))
      } else None
    }
  }

  def parse(bytes: Array[Byte]): Option[W3CTraceparent] =
    if (bytes == null) None
    else parse(new String(bytes, StandardCharsets.UTF_8))
}

object TraceHeaders {
  val TraceparentHeader = "traceparent"
  val TracestateHeader = "tracestate"

  /** Extracts W3C Traceparent from Kafka Headers */
  def extractTraceparent(headers: Headers): Option[W3CTraceparent] = {
    Option(headers.lastHeader(TraceparentHeader))
      .flatMap(h => W3CTraceparent.parse(h.value()))
  }

  /** Injects W3C Traceparent into Kafka Headers */
  def injectTraceparent(headers: Headers, tp: W3CTraceparent): Headers = {
    headers.remove(TraceparentHeader)
    headers.add(new RecordHeader(TraceparentHeader, tp.asBytes))
    headers
  }

  /** Extracts Tracestate string from Kafka Headers */
  def extractTracestate(headers: Headers): Option[String] = {
    Option(headers.lastHeader(TracestateHeader))
      .map(h => new String(h.value(), StandardCharsets.UTF_8))
  }

  /** Injects Tracestate string into Kafka Headers */
  def injectTracestate(headers: Headers, state: String): Headers = {
    headers.remove(TracestateHeader)
    headers.add(new RecordHeader(TracestateHeader, state.getBytes(StandardCharsets.UTF_8)))
    headers
  }

  implicit class ProducerRecordTraceOps[K, V](pr: ProducerRecord[K, V]) {
    def traceparent: Option[W3CTraceparent] =
      extractTraceparent(pr.headers())

    def withTraceparent(tp: W3CTraceparent): ProducerRecord[K, V] = {
      injectTraceparent(pr.headers(), tp)
      pr
    }

    def withTraceparent(raw: String): Option[ProducerRecord[K, V]] =
      W3CTraceparent.parse(raw).map(withTraceparent)

    def tracestate: Option[String] =
      extractTracestate(pr.headers())

    def withTracestate(state: String): ProducerRecord[K, V] = {
      injectTracestate(pr.headers(), state)
      pr
    }
  }

  implicit class ConsumerRecordTraceOps[K, V](cr: ConsumerRecord[K, V]) {
    def traceparent: Option[W3CTraceparent] =
      extractTraceparent(cr.headers())

    def tracestate: Option[String] =
      extractTracestate(cr.headers())
  }
}
