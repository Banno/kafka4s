package com.banno.kafka.connect

import org.apache.kafka.connect.source.{SourceRecord => KCSourceRecord}
import scala.collection.JavaConverters._
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.header.Header

case class SourceRecord[P: MapEncoder, O: MapEncoder](
    sourcePartition: P,
    sourceOffset: O,
    topic: String,
    partition: Int,
    keySchema: Schema,
    key: Object,
    valueSchema: Schema,
    value: Object,
    timestamp: Long,
    headers: Iterable[Header]
) {
  def toSourceRecord: KCSourceRecord = new KCSourceRecord(
    MapEncoder[P].encode(sourcePartition).asJava,
    MapEncoder[O].encode(sourceOffset).asJava,
    topic,
    partition,
    keySchema,
    key,
    valueSchema,
    value,
    timestamp,
    headers.asJava
  )
}

//TODO replicate all KCSourceRecord constructors
object SourceRecord {
  def apply[P: MapEncoder, O: MapEncoder](
      sourcePartition: P,
      sourceOffset: O,
      topic: String,
      keySchema: Schema,
      key: Object,
      valueSchema: Schema,
      value: Object
  ): SourceRecord[P, O] =
    SourceRecord[P, O](
      sourcePartition,
      sourceOffset,
      topic,
      null.asInstanceOf[Int],
      keySchema,
      key,
      valueSchema,
      value,
      null.asInstanceOf[Long],
      null
    )
}
