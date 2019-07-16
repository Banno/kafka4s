package com.banno.kafka

import org.scalatest._
import org.scalacheck._
import cats.implicits._
import cats.effect._
import consumer._
import producer._
import org.apache.kafka.clients.producer.ProducerRecord

import scala.concurrent.duration._
import com.banno.kafka.producer._
import com.banno.kafka.consumer._
import fs2._

import scala.concurrent.ExecutionContext
import java.util.concurrent.Executors

import cats.effect.concurrent.Ref

import scala.util.Random
import com.mrdziuban.ScalacheckMagnolia._
import org.apache.kafka.common.TopicPartition
import org.scalatestplus.scalacheck._

import scala.collection.JavaConverters._
import java.util.ConcurrentModificationException
import org.apache.kafka.common.errors.WakeupException

class ConsumerAndProducerApiSpec
    extends PropSpec
    with ScalaCheckDrivenPropertyChecks
    with Matchers
    with EitherValues
    with InMemoryKafka {
  val producerContext = ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(2))
  val consumerContext = ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(2))
  implicit val defaultContextShift = IO.contextShift(ExecutionContext.global)
  implicit val defaultConcurrent = IO.ioConcurrentEffect(defaultContextShift)
  implicit val defaultTimer = IO.timer(ExecutionContext.global)

  implicit def noShrink[T]: Shrink[T] = Shrink.shrinkAny

  //Probably don't need to test every single operation; this is just a sanity check that it is all wired up properly

  def producerRecord[K, V](topic: String)(p: (K, V)): ProducerRecord[K, V] =
    new ProducerRecord[K, V](topic, p._1, p._2)

  def writeAndRead[F[_]: Effect, K, V](
      producer: ProducerApi[F, K, V],
      consumer: ConsumerApi[F, K, V],
      topic: String,
      values: Vector[(K, V)]
  ): Stream[F, (K, V)] =
    Stream
      .emits(values)
      .covary[F]
      .map(producerRecord(topic))
      .evalMap(producer.sendAndForget)
      .drain ++
      Stream.eval(consumer.subscribe(topic)).drain ++
      consumer
        .recordStream(100 millis)
        .map(cr => (cr.key, cr.value))
        .take(values.size.toLong)

  property("Producer API sends records and produces metadata") {
    val topic = createTopic()

    forAll { strings: Vector[String] =>
      //TODO what does sendAsyncBatch actually do?
      //  1. send one record and semantically block until metadata received, send next record and semantically block until metadata received, etc
      //  2. or, send all records in batch, then semantically block until all metadatas received
      //I suspect it's #1, which may be useful, but most of the time #2 is really what we want, for max throughput

      val rms = ProducerApi
        .resource[IO, String, String](BootstrapServers(bootstrapServer))
        .use(_.sendAsyncBatch(strings.map(s => new ProducerRecord(topic, s, s))))
        .unsafeRunSync()

      rms.size should ===(strings.size)
      rms.forall(_.topic == topic) should ===(true)
      if (strings.size >= 2) {
        rms.last.offset - rms.head.offset should ===(strings.size - 1)
      }
    }
  }

  //KafkaConsumer is not thread-safe; if one thread is calling poll while another concurrently calls close, close will throw ConcurrentModificationException
  property("Simple consumer close fails with ConcurrentModificationException while polling") {
    val topic = createTopic()
    (for {
      c <- ConsumerApi.create[IO, String, String](BootstrapServers(bootstrapServer))
      _ <- c.assign(topic, Map.empty[TopicPartition, Long])
      _ <- Concurrent[IO].start(c.poll(1 second) *> c.close)
      e <- Timer[IO].sleep(100 millis) *> c.close.attempt
    } yield {
      e.left.value shouldBe a[ConcurrentModificationException]
    }).unsafeRunSync()
  }

  //Calling KafkaConsumer.wakeup will cause any other concurrent operation to throw WakeupException
  property("Simple consumer poll fails with WakeupException on wakeup") {
    val topic = createTopic()
    (for {
      c <- ConsumerApi.create[IO, String, String](BootstrapServers(bootstrapServer))
      _ <- c.assign(topic, Map.empty[TopicPartition, Long])
      _ <- Concurrent[IO].start(Timer[IO].sleep(100 millis) *> c.wakeup)
      e <- c.poll(1 second).attempt
      _ <- c.close
    } yield {
      e.left.value shouldBe a[WakeupException]
    }).unsafeRunSync()
  }

  //If we recover from close failing with CME by calling wakeup, and recover from poll failing with WE by calling close, we can call poll and close concurrently
  property("Simple consumer close while polling") {
    val topic = createTopic()
    (for {
      c <- ConsumerApi.create[IO, String, String](BootstrapServers(bootstrapServer))
      _ <- c.assign(topic, Map.empty[TopicPartition, Long])
      f <- Concurrent[IO].start(c.pollAndRecoverWakeupWithClose(1 second))
      e1 <- Timer[IO].sleep(100 millis) *> c.closeAndRecoverConcurrentModificationWithWakeup.attempt
      e2 <- f.join.attempt
    } yield {
      e1.right.value should ===(())
      e2.right.value.count should ===(0)
    }).unsafeRunSync()
  }

  //If we shift all operations onto a singleton thread pool then they become sequential, so we can safely call poll and close concurrently without requiring recovery & wakeup
  property("Singleton shifting consumer close while polling") {
    val topic = createTopic()
    val ctx = ExecutionContext.fromExecutorService(Executors.newSingleThreadExecutor())
    (for {
      c <- ConsumerApi.createShifting[IO, String, String](ctx, BootstrapServers(bootstrapServer))
      _ <- c.assign(topic, Map.empty[TopicPartition, Long])
      _ <- Concurrent[IO].start(c.poll(1 second))
      e <- Timer[IO].sleep(100 millis) *> c.close.attempt
    } yield {
      e.right.value should ===(())
    }).unsafeRunSync()
    ctx.shutdown()
  }

  //wakeup is the one thread-safe operation, so we don't need to shift it
  property("Singleton shifting consumer poll fails with WakeupException on wakeup") {
    val topic = createTopic()
    val ctx = ExecutionContext.fromExecutorService(Executors.newSingleThreadExecutor())
    (for {
      c <- ConsumerApi.createShifting[IO, String, String](ctx, BootstrapServers(bootstrapServer))
      _ <- c.assign(topic, Map.empty[TopicPartition, Long])
      _ <- Concurrent[IO].start(Timer[IO].sleep(100 millis) *> c.wakeup)
      e <- c.poll(1 second).attempt
      _ <- c.close
    } yield {
      e.left.value shouldBe a[WakeupException]
    }).unsafeRunSync()
    ctx.shutdown()
  }

  property("Producer and Consumer APIs should write and read records") {
    val groupId = genGroupId
    println(s"2 groupId=$groupId")
    val topic = createTopic()

    forAll { values: Vector[(String, String)] =>
      val actual = (for {
        p <- ProducerApi.stream[IO, String, String](BootstrapServers(bootstrapServer))
        c <- ConsumerApi.stream[IO, String, String](
          BootstrapServers(bootstrapServer),
          GroupId(groupId),
          AutoOffsetReset.earliest
        )
        x <- writeAndRead(p, c, topic, values)
      } yield x).compile.toVector.unsafeRunSync()
      actual should ===(values)
    }
  }

  property("Write and read in blocking context") {
    val groupId = genGroupId
    println(s"3 groupId=$groupId")
    val topic = createTopic()

    forAll { values: Vector[(Int, String)] =>
      val actual = (for {
        p <- ProducerApi.streamShifting[IO, Int, String](
          producerContext,
          BootstrapServers(bootstrapServer)
        )
        c <- ConsumerApi.streamShifting[IO, Int, String](
          consumerContext,
          BootstrapServers(bootstrapServer),
          GroupId(groupId),
          AutoOffsetReset.earliest
        )
        x <- writeAndRead(p, c, topic, values)
      } yield x).compile.toVector.unsafeRunSync()
      actual should ===(values)
    }
  }

  property("read through final offsets") {
    val topic = createTopic(3)

    val data = Map(
      0 -> List("0-0", "0-1"),
      1 -> List("1-0", "1-1"),
      2 -> List("2-0", "2-1")
    )
    val records = data.toList.flatMap {
      case (p, vs) => vs.map(v => new ProducerRecord(topic, p, v, v))
    }
    val tp0 = new TopicPartition(topic, 0)
    val tp1 = new TopicPartition(topic, 1)
    val tp2 = new TopicPartition(topic, 2)

    ProducerApi
      .resource[IO, String, String](BootstrapServers(bootstrapServer))
      .use(
        p =>
          //max.poll.records=1 forces stream to repeat a few times, so we validate the takeThrough predicate
          ConsumerApi
            .resource[IO, String, String](BootstrapServers(bootstrapServer), MaxPollRecords(1))
            .use(
              c =>
                for {
                  _ <- p.sendSyncBatch(records)

                  _ <- c.assign(List(tp0, tp1, tp2))
                  _ <- c.seekToBeginning(List(tp0))
                  _ <- c.seek(tp1, 1)
                  _ <- c.seekToEnd(List(tp2))
                  vs <- c
                    .recordsThroughOffsets(Map(tp0 -> 1, tp1 -> 1, tp2 -> 1), 1 second)
                    .map(_.asScala.map(_.value))
                    .compile
                    .toList

                  //consumer must still be usable after stream halts, positioned immediately after all of the records it's already returned
                  _ <- p.sendSync(new ProducerRecord(topic, null, "new"))
                  rs <- c.poll(1 second)

                  _ <- p.close
                  _ <- c.close
                } yield {
                  (vs.flatten should contain).theSameElementsAs(List("0-0", "0-1", "1-1"))
                  rs.asScala.map(_.value) should ===(List("new"))
                }
            )
      )
      .unsafeRunSync()
  }

  property("readProcessCommit only commits offsets for successfully processed records") {
    val groupId = genGroupId
    println(s"4 groupId=$groupId")
    val topic = createTopic()

    //simulates some effect that might fail, e.g. HTTP POST, DB write, etc
    def storeOrFail(values: Ref[IO, Vector[String]], s: String): IO[String] =
      if (Random.nextDouble() < 0.7)
        IO(println(s"success: $s")) *> values.update(_ :+ s).as(s)
      else
        IO(println(s"fail:    $s")) *> IO.raiseError(new RuntimeException("fail"))

    val expected = Vector.range(0, 10).map(_.toString)
    val io = for {
      values <- Ref.of[IO, Vector[String]](Vector.empty)
      _ <- ProducerApi
        .resource[IO, String, String](BootstrapServers(bootstrapServer))
        .use(_.sendSyncBatch(expected.map(s => new ProducerRecord(topic, s, s))))
      consume: Stream[IO, String] = ConsumerApi
        .stream[IO, String, String](
          BootstrapServers(bootstrapServer),
          GroupId(groupId),
          AutoOffsetReset.earliest,
          EnableAutoCommit(false)
        )
        .evalTap(_.subscribe(topic))
        .flatMap(_.readProcessCommit(100 millis)(r => storeOrFail(values, r.value))) //only consumes until a failure)
      _ <- consume.attempt.repeat
        .takeThrough(_ != Right(expected.last))
        .compile
        .drain //keeps consuming until last record is successfully processed
      vs <- values.get
    } yield vs
    val actual = io.unsafeRunSync()
    actual should ===(expected) //verifies that no successfully processed record was ever reprocessed
  }

  property("Producer transaction works") {
    pending
  }

  property("Avro serdes") {
    val groupId = genGroupId
    println(s"5 groupId=$groupId")
    val topic = createTopic()

    forAll { values: Vector[(String, Person)] =>
      val actual = (for {
        p <- ProducerApi.streamAvro[IO, String, Person](
          BootstrapServers(bootstrapServer),
          SchemaRegistryUrl(schemaRegistryUrl)
        )
        c <- ConsumerApi.streamAvroSpecific[IO, String, Person](
          BootstrapServers(bootstrapServer),
          GroupId(groupId),
          AutoOffsetReset.earliest,
          SchemaRegistryUrl(schemaRegistryUrl)
        )
        v <- writeAndRead(p, c, topic, values)
      } yield v).compile.toVector.unsafeRunSync()
      actual should ===(values)
    }
  }

  //for avro4s tests
  case class PersonId(id: String)
  case class Person2(name: String)

  property("avro4s") {
    val groupId = genGroupId
    println(s"6 groupId=$groupId")
    val topic = createTopic()

    forAll { values: Vector[(PersonId, Person2)] =>
      val actual = (for {
        p <- ProducerApi.streamAvro4s[IO, PersonId, Person2](
          BootstrapServers(bootstrapServer),
          SchemaRegistryUrl(schemaRegistryUrl)
        )
        c <- ConsumerApi.streamAvro4s[IO, PersonId, Person2](
          BootstrapServers(bootstrapServer),
          GroupId(groupId),
          AutoOffsetReset.earliest,
          SchemaRegistryUrl(schemaRegistryUrl)
        )
        v <- writeAndRead(p, c, topic, values)
      } yield v).compile.toVector.unsafeRunSync()
      actual should ===(values)
    }
  }

}
