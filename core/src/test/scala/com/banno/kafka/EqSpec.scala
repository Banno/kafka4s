package com.banno.kafka

import cats.kernel.laws.discipline.EqTests
import com.banno.kafka.test._
import munit._
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord

class EqSpec extends DisciplineSuite {
  checkAll("Eq[ProducerRecord]", EqTests[ProducerRecord[Int, String]].eqv)
  checkAll("Eq[ConsumerRecord]", EqTests[ConsumerRecord[Int, String]].eqv)
}
