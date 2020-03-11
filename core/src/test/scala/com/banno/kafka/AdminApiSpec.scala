package com.banno.kafka

import com.banno.kafka.admin._
import cats.effect._
import cats.implicits._
import org.apache.kafka.clients.admin._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class AdminApiSpec extends AnyFlatSpec with Matchers with InMemoryKafka {

  //Probably don't need to test every single AdminClient operation; this is just a sanity check that it is all wired up properly

  "Admin API" should "create topics idempotently" in {
    def program[F[_]](admin: AdminApi[F])(implicit F: Sync[F]) =
      for {
        ltr1 <- admin.listTopics
        ns1 <- F.delay(ltr1.names().get())
        _ <- admin.createTopicsIdempotent(List(new NewTopic("test1", 1, 1.toShort)))
        _ <- admin.createTopicsIdempotent(List(new NewTopic("test1", 1, 1.toShort)))
        ltr2 <- admin.listTopics
        ns2 <- F.delay(ltr2.names.get())
      } yield (ns1, ns2)

    val (before, after) =
      AdminApi.resource[IO](BootstrapServers(bootstrapServer)).use(program[IO]).unsafeRunSync()
    before should not contain ("test1")
    after should contain("test1")
  }

}
