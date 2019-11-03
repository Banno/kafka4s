package com.banno.kafka.connect

import scala.concurrent.duration._
import io.circe.{Decoder => CirceDecoder}

trait StringDecoder[A] {
  def decode(s: String): A
}

object StringDecoder {

  def apply[A](implicit A: StringDecoder[A]): StringDecoder[A] = A

  def stringDecoder[A](f: String => A): StringDecoder[A] = new StringDecoder[A] {
    override def decode(s: String): A = f(s)
  }

  implicit val string: StringDecoder[String] = stringDecoder(identity)
  implicit val boolean: StringDecoder[Boolean] = stringDecoder(_.toBoolean)
  implicit val int: StringDecoder[Int] = stringDecoder(_.toInt)
  implicit val long: StringDecoder[Long] = stringDecoder(_.toLong)
  implicit val float: StringDecoder[Float] = stringDecoder(_.toFloat)
  implicit val double: StringDecoder[Double] = stringDecoder(_.toDouble)
  implicit val short: StringDecoder[Short] = stringDecoder(_.toShort)
  implicit val byte: StringDecoder[Byte] = stringDecoder(_.toByte)
  implicit val finiteDuration: StringDecoder[FiniteDuration] = stringDecoder { s =>
    val d = Duration(s)
    if (d.isFinite) FiniteDuration(d.length, d.unit)
    else throw new RuntimeException(s"$s is not a finite duration")
  }

  implicit def option[A: StringDecoder]: StringDecoder[Option[A]] =
    stringDecoder(Option(_).map(StringDecoder[A].decode))

  implicit def circe[T: CirceDecoder]: StringDecoder[T] =
    stringDecoder(io.circe.jawn.decode[T](_).fold(e => throw e, identity))

  //UUID
  //LocalTime
  //LocalDate
  //Instant
  //LocalDateTime
  //Map
  //List, Seq, Vector
  //Array[Byte]
  //Option
  //Either
  //BigDecimal
  //Java enum
  //Scala enum
  //CNil and Coproduct
}
