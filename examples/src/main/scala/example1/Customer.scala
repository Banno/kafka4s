package example1

import com.sksamuel.avro4s._

final case class Customer(name: String, address: String)
object Customer {
  implicit def customerRecordFormat = RecordFormat[Customer]
}
