package com.banno.kafka

import org.scalatest.{FunSuite, Matchers}
import cats.kernel.laws.discipline.EqTests
import org.typelevel.discipline.scalatest.Discipline
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.consumer.ConsumerRecord
import com.banno.kafka.test._
// import org.scalacheck.Arbitrary

class EqSpec extends FunSuite with Matchers with Discipline {
  checkAll("Eq[ProducerRecord]", EqTests[ProducerRecord[Int, String]].eqv)
  checkAll("Eq[ConsumerRecord]", EqTests[ConsumerRecord[Int, String]].eqv)
}
