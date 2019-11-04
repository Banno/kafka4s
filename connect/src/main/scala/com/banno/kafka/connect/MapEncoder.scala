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

package com.banno.kafka.connect

import shapeless._
import shapeless.labelled.FieldType

trait MapEncoder[A] {
  def encode(a: A): Map[String, String]
}

object MapEncoder {

  def apply[A](implicit A: MapEncoder[A]): MapEncoder[A] = A

  def mapEncoder[A](f: A => Map[String, String]): MapEncoder[A] = new MapEncoder[A] {
    override def encode(a: A): Map[String, String] = f(a)
  }

  implicit def hnilEncoder: MapEncoder[HNil] =
    mapEncoder(_ => Map.empty)

  implicit def hlistEncoder[K <: Symbol, H: StringEncoder, T <: HList](
      implicit witness: Witness.Aux[K],
      tEncoder: MapEncoder[T]
  ): MapEncoder[FieldType[K, H] :: T] =
    mapEncoder(
      a => Map(witness.value.name -> StringEncoder[H].encode(a.head)) ++ tEncoder.encode(a.tail)
    )

  implicit def genericMapEncoder[A, R <: HList](
      implicit gen: LabelledGeneric.Aux[A, R],
      rEncoder: MapEncoder[R]
  ): MapEncoder[A] =
    mapEncoder(a => rEncoder.encode(gen.to(a)).filter { case (_, v) => v != null })
}
