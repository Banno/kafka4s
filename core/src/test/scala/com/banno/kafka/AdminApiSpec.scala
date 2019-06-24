package com.banno.kafka

import org.scalatest._
import com.banno.kafka.admin._
import cats.effect._
import cats.implicits._
import org.apache.kafka.clients.admin._

class AdminApiSpec extends FlatSpec with Matchers with InMemoryKafka {

  //Probably don't need to test every single AdminClient operation; this is just a sanity check that it is all wired up properly

  "Admin API" should "create topics idempotently" in {
    def program[F[_]](admin: AdminApi[F])(implicit F: Sync[F]) = 
      for {
        ltr1 <- admin.listTopics
        ns1 <- F.delay(ltr1.names().get())
        _ <- admin.createTopicsIdempotent(List(new NewTopic("test1", 1, 1)))
        _ <- admin.createTopicsIdempotent(List(new NewTopic("test1", 1, 1)))
        ltr2 <- admin.listTopics
        ns2 <- F.delay(ltr2.names.get())
        _ <- admin.close
      } yield (ns1, ns2)

    val io = for {
      admin <- AdminApi[IO](BootstrapServers(bootstrapServer))
      results <- program(admin)
    } yield results
    val (before, after) = io.unsafeRunSync()
    before should not contain ("test1")
    after should contain ("test1")
  }

}
