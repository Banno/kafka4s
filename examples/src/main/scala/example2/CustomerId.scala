package example2

import com.sksamuel.avro4s._

object CustomerId { implicit def customerIdRecordFormat = RecordFormat[CustomerId] }
final case class CustomerId(id: String)
