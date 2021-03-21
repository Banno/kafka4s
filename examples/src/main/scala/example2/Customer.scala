package example2

import com.sksamuel.avro4s._

final case class Customer(name: String, address: String, priority: Option[Priority] = None)
object Customer {
  implicit def customerRecordFormat = RecordFormat[Customer]
}
