package com.banno.kafka

import scala.util._

trait Serialize[A] {
  def toByteArray(x: A): Array[Byte]
}

object Serialize {
  def apply[A](implicit ev: Serialize[A]): Serialize[A] = ev
}

trait Deserialize[A] {
  def fromByteArray(x: Array[Byte]): Try[A]
}

object Deserialize {
  def apply[A](implicit ev: Deserialize[A]): Deserialize[A] = ev
}

trait Serde[A] extends Serialize[A] with Deserialize[A]

object Serde {
  def apply[A](implicit ev: Serde[A]): Serde[A] = ev
}
