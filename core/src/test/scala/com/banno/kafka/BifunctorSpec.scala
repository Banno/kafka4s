package com.banno.kafka

import cats.laws.discipline.BifunctorTests
import org.typelevel.discipline.scalatest.Discipline
import com.banno.kafka.test._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class BifunctorSpec extends AnyFunSuite with Matchers with Discipline {
  checkAll(
    "ProducerRecordBifunctor",
    BifunctorTests(ProducerRecordBifunctor).bifunctor[Int, Int, Int, String, String, String]
  )
  checkAll(
    "ConsumerRecordBifunctor",
    BifunctorTests(ConsumerRecordBifunctor).bifunctor[Int, Int, Int, String, String, String]
  )
}
