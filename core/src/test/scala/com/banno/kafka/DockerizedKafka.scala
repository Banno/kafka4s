package com.banno.kafka

import cats.effect._
import cats.syntax.all._
import org.scalacheck.Gen
import com.banno.kafka.admin.AdminApi
import cats.effect.IO
import org.typelevel.log4cats.slf4j.Slf4jLogger
import org.apache.kafka.clients.admin.NewTopic

trait DockerizedKafka {
  val bootstrapServer = "localhost:9092"
  val schemaRegistryUrl = "http://localhost:8091"

  def unsafeRandomId: String =
    Gen.listOfN(10, Gen.alphaChar).map(_.mkString).sample.get

  def createTopic[F[_]: Sync](partitionCount: Int = 1): F[String] =
    for {
      topic <- Sync[F].delay(unsafeRandomId)
      _ <- AdminApi.createTopicsIdempotent[F](
        bootstrapServer,
        List(new NewTopic(topic, partitionCount, 1.toShort))
      )
    } yield topic

}
