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
