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
          .decode(m.getOrElse(name, throw new RuntimeException(s"Map does not contain $name: ${m.keys}")))
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
