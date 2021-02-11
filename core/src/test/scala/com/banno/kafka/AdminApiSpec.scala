package com.banno.kafka

import com.banno.kafka.admin._
import cats.effect._
import cats.syntax.all._
import munit.CatsEffectSuite
import org.apache.kafka.clients.admin._
import org.scalacheck.Gen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.util
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import com.banno.kafka.test.{utils => TestUtils} // TODO

class AdminApiSpec extends CatsEffectSuite {
  val cp = ResourceFixture(ConfluentContainers.resource[IO])

  def program[F[_]: Sync: Timer](
      topicName: String
  )(admin: AdminApi[F]): F[(util.Set[String], util.Set[String])] =
    for {
      ltr1 <- admin.listTopics
      ns1 <- Sync[F].delay(ltr1.names().get())
      _ <- admin.createTopicsIdempotent(List(new NewTopic(topicName, 1, 1.toShort)))
      _ <- admin.createTopicsIdempotent(List(new NewTopic(topicName, 1, 1.toShort)))
      _ <- Timer[F].sleep(1.second) // TODO: Better fix
      ltr2 <- admin.listTopics
      ns2 <- Sync[F].delay(ltr2.names.get())
    } yield (ns1, ns2)

  cp.test("Admin API creates topics idempotently") { cp =>
    val a = for {
      topicName <- Sync[IO].delay(TestUtils.genTopic())
      bs <- cp.bootstrapServers
      (before, after) <- AdminApi.resource[IO](BootstrapServers(bs)).use(program[IO](topicName))
    } yield (before.contains(topicName), after.contains(topicName))

    a.assertEquals((false, true))

  }
}
