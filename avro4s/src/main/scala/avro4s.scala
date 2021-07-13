package com.banno.kafka

import scala.util._
import com.sksamuel.avro4s._
import io.confluent.kafka.schemaregistry._
import io.confluent.kafka.schemaregistry.avro.AvroSchema

package object avro4s {
  implicit def schematic[A](implicit
    T: ToRecord[A],
    F: FromRecord[A],
    S: SchemaFor[A],
  ): Schematic[A] =
    new Schematic[A] {
      override def fromByteArray(x: Array[Byte]): Try[A] = {
        Try(F.from(null)) // TODO
      }

      override def schema: ParsedSchema =
        new AvroSchema(S.schema(DefaultFieldMapper))

      override def toByteArray(x: A): Array[Byte] = {
        val _ = T.to(x)
        ???
      }
    }
}
