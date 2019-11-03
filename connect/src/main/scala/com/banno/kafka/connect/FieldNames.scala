package com.banno.kafka.connect

import shapeless._
import shapeless.labelled.FieldType

case class FieldName[A](name: String)

/** Type class providing the key names and value types of a record or case class as an `HList`. */
trait FieldNames[L] extends DepFn0 with Serializable { type Out <: HList }

object FieldNames {

  type Aux[L, Out0 <: HList] = FieldNames[L] { type Out = Out0 }

  def apply[L](implicit f: FieldNames[L]): Aux[L, f.Out] = f

  implicit def hnilFieldNames[L <: HNil]: Aux[L, HNil] =
    new FieldNames[L] {
      type Out = HNil
      def apply(): Out = HNil
    }

  implicit def hlistFieldNames[K <: Symbol, V, T <: HList](
      implicit wk: Witness.Aux[K],
      ft: FieldNames[T]
  ): Aux[FieldType[K, V] :: T, FieldName[V] :: ft.Out] =
    new FieldNames[FieldType[K, V] :: T] {
      type Out = FieldName[V] :: ft.Out
      def apply(): Out = FieldName[V](wk.value.name) :: ft()
    }

  implicit def genericFieldNames[A, R <: HList, O <: HList](
      implicit gen: LabelledGeneric.Aux[A, R],
      fieldNames: FieldNames.Aux[R, O]
  ): FieldNames.Aux[A, O] =
    new FieldNames[A] {
      val _ = gen //convince compiler we need this
      type Out = O
      def apply(): Out = fieldNames()
    }
}