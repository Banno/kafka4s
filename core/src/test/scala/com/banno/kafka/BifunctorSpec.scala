package com.banno.kafka

import org.scalatest.{FunSuite, Matchers}
import cats.laws.discipline.BifunctorTests
import org.typelevel.discipline.scalatest.Discipline
import com.banno.kafka.test._

class BifunctorSpec extends FunSuite with Matchers with Discipline {
  checkAll(
    "ProducerRecordBifunctor",
    BifunctorTests(ProducerRecordBifunctor).bifunctor[Int, Int, Int, String, String, String])
  checkAll(
    "ConsumerRecordBifunctor",
    BifunctorTests(ConsumerRecordBifunctor).bifunctor[Int, Int, Int, String, String, String])
}
