/*
 * Copyright 2019 Jack Henry & Associates, Inc.®
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
