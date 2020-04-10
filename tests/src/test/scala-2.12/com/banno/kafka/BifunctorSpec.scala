package com.banno.kafka

import cats.laws.discipline.BifunctorTests
import com.banno.kafka.test._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.Checkers
import org.typelevel.discipline.scalatest.FunSuiteDiscipline

class BifunctorSpec extends AnyFunSuite with Matchers with Checkers with FunSuiteDiscipline {
  checkAll(
    "ProducerRecordBifunctor",
    BifunctorTests(ProducerRecordBifunctor).bifunctor[Int, Int, Int, String, String, String]
  )
  checkAll(
    "ConsumerRecordBifunctor",
    BifunctorTests(ConsumerRecordBifunctor).bifunctor[Int, Int, Int, String, String, String]
  )
}
