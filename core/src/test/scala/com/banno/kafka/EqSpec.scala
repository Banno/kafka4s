package com.banno.kafka

import cats.kernel.laws.discipline.EqTests
import com.banno.kafka.test._
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.Checkers
import org.typelevel.discipline.scalatest.FunSuiteDiscipline

class EqSpec extends AnyFunSuite with Matchers with Checkers with FunSuiteDiscipline {
  checkAll("Eq[ProducerRecord]", EqTests[ProducerRecord[Int, String]].eqv)
  checkAll("Eq[ConsumerRecord]", EqTests[ConsumerRecord[Int, String]].eqv)
}
