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
import shapeless.labelled.{FieldType, field}

trait MapDecoder[A] {
  def decode(m: Map[String, String]): A
}

object MapDecoder {

  def apply[A](implicit A: MapDecoder[A]): MapDecoder[A] = A

  def mapDecoder[A](f: Map[String, String] => A): MapDecoder[A] = new MapDecoder[A] {
    override def decode(m: Map[String, String]): A = f(m)
  }

  implicit def hnilMapDecoder: MapDecoder[HNil] = mapDecoder(_ => HNil)

  implicit def hlistMapDecoder[K <: Symbol, H: StringDecoder, T <: HList](
      implicit witness: Witness.Aux[K],
      tDecoder: MapDecoder[T]
  ): MapDecoder[FieldType[K, H] :: T] =
    mapDecoder[FieldType[K, H] :: T] { m =>
      val name = witness.value.name
      field[K](
        StringDecoder[H]
          .decode(
            m.getOrElse(name, throw new RuntimeException(s"Map does not contain $name: ${m.keys}"))
          )
      ) :: tDecoder.decode(m)
    }

  implicit def genericMapDecoder[A, R <: HList, D <: HList](
      implicit gen: LabelledGeneric.Aux[A, R],
      defaultAsRecord: Default.AsRecord.Aux[A, D],
      dEncoder: MapEncoder[D],
      rDecoder: MapDecoder[R]
  ): MapDecoder[A] = {
    val defaultValues = dEncoder.encode(defaultAsRecord())
    mapDecoder(m => gen.from(rDecoder.decode(defaultValues ++ m)))
  }
}
