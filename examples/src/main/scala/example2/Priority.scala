package example2

import com.sksamuel.avro4s._

sealed trait Priority
object Priority {
  implicit def priorityRecordFormat = RecordFormat[Priority]
  case object Platinum extends Priority
  case object Gold extends Priority
}
