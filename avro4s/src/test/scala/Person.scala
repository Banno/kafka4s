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

import scala.annotation.switch

case class Person(var name: String) extends org.apache.avro.specific.SpecificRecordBase {
  def this() = this("")
  def get(field$ : Int): AnyRef =
    (field$ : @switch) match {
      case 0 => {
        name
      }.asInstanceOf[AnyRef]
      case _ => new org.apache.avro.AvroRuntimeException("Bad index")
    }
  def put(field$ : Int, value: Any): Unit = {
    (field$ : @switch) match {
      case 0 =>
        this.name = {
          value.toString
        }.asInstanceOf[String]
      case _ => new org.apache.avro.AvroRuntimeException("Bad index")
    }
    ()
  }
  def getSchema: org.apache.avro.Schema = Person.SCHEMA$
}

object Person {
  val SCHEMA$ = new org.apache.avro.Schema.Parser().parse(
    "{\"type\":\"record\",\"name\":\"Person\",\"namespace\":\"com.banno.kafka\",\"fields\":[{\"name\":\"name\",\"type\":\"string\"}]}"
  )
}
