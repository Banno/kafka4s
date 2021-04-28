package com.banno.kafka

import cats.laws.discipline.BifunctorTests
import com.banno.kafka.test._
import munit._

class BifunctorSpec extends DisciplineSuite {
  checkAll(
    "ProducerRecordBifunctor",
    BifunctorTests(ProducerRecordBifunctor).bifunctor[Int, Int, Int, String, String, String]
  )
  checkAll(
    "ConsumerRecordBifunctor",
    BifunctorTests(ConsumerRecordBifunctor).bifunctor[Int, Int, Int, String, String, String]
  )
}
