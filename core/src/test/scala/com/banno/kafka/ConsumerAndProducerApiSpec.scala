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

class ConsumerAndProducerApiSpec extends PropSpec with ScalaCheckDrivenPropertyChecks with Matchers with InMemoryKafka {
  val producerContext = ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(2))
  implicit val defaultContextShift = IO.contextShift(ExecutionContext.global)
  implicit val defaultConcurrent = IO.ioConcurrentEffect(defaultContextShift)
  implicit val defaultTimer = IO.timer(ExecutionContext.global)

  implicit def noShrink[T]: Shrink[T] = Shrink.shrinkAny

  //Probably don't need to test every single operation; this is just a sanity check that it is all wired up properly

  def producerRecord[K, V](topic: String)(p: (K, V)): ProducerRecord[K, V] = 
    new ProducerRecord[K, V](topic, p._1, p._2)

  def writeAndRead[F[_]: Effect, K, V](producer: ProducerApi[F, K, V], consumer: ConsumerApi[F, K, V], topic: String, values: Vector[(K, V)]): Stream[F, (K, V)] = 
    Stream.emits(values).covary[F].map(producerRecord(topic)).through(producer.sinkWithClose).drain ++
      consumer.recordStream(consumer.subscribe(topic), 100 millis).map(cr => (cr.key, cr.value)).take(values.size.toLong)

  property("Producer API sends records and produces metadata") {
    val topic = createTopic()

    forAll { strings: Vector[String] =>
      val s = for {
        producer <- Stream.eval(ProducerApi.create[IO, String, String](BootstrapServers(bootstrapServer)))
        rm <- Stream.emits(strings).covary[IO].map(s => new ProducerRecord(topic, s, s)).through(producer.pipeSync).onFinalize(producer.close)
      } yield rm
      val rms = s.compile.toVector.unsafeRunSync()

      rms.size should === (strings.size)
      rms.forall(_.topic == topic) should === (true)
      if (strings.size >= 2) {
        rms.last.offset - rms.head.offset should === (strings.size - 1)
      }
    }
  }

  property("Producer sendSyncBatch") {
    val topic = createTopic()

    forAll { strings: Vector[(String, String)] =>
      val io = for {
        producer <- ProducerApi.create[IO, String, String](BootstrapServers(bootstrapServer))
        rms <- producer.sendSyncBatch(strings.map{case (k, v) => new ProducerRecord(topic, k, v)})
        _ <- producer.close
      } yield rms
      val rms = io.unsafeRunSync()

      rms.size should === (strings.size)
      rms.forall(_.topic == topic) should === (true)
      if (strings.size >= 2) {
        rms.last.offset - rms.head.offset should === (strings.size - 1)
      }
    }
  }

  property("Consumer API can be interrupted by another thread") {

    val topic = createTopic()
    val groupId = genGroupId
    println(s"1 groupId=$groupId")

    val io2 = for {
      c <- ConsumerApi.create[IO, String, String](BootstrapServers(bootstrapServer), GroupId(groupId), AutoOffsetReset.earliest)
      _ <- c.subscribe(topic)
      _ <- Concurrent[IO].start(c.pollAndRecoverWakeupWithClose(10 seconds))
      _ <- IO(Thread.sleep(1000)) *> c.closeAndRecoverConcurrentModificationWithWakeup(1 second)
    } yield 123
    io2.unsafeRunSync() should === (123)

    val groupId2 = genGroupId

    def program[F[_]: ConcurrentEffect: Timer](producer: ProducerApi[F, String, String], consumer: ConsumerApi[F, String, String]): Stream[F, String] = {
      val s0 = Stream(new ProducerRecord(topic, "a", "a")).covary[F].through(producer.sinkWithClose) //halts after emitting "a" to Kafka
      val s1 = consumer.recordStream(consumer.subscribe(topic), 1 second).map(_.value) //consumes from kafka, never halting
      val s2 = Stream("b").covary[F].delayBy(5 seconds) //after 5 seconds, 1 element will be emitted and this stream will halt
      for {
        x <- s2.mergeHaltL(s0.drain ++ s1) //s1 is running in the background with poll() called by thread 1, then s2 halts and close() called on thread 2
      } yield x
    }
    
    val io = for {
      p <- ProducerApi.create[IO, String, String](BootstrapServers(bootstrapServer))
      c <- ConsumerApi.create[IO, String, String](BootstrapServers(bootstrapServer), GroupId(groupId2), AutoOffsetReset.earliest)
      v <- program[IO](p, c).compile.toVector
    } yield v
    io.unsafeRunSync().toSet should === (Set("a", "b"))
  }

  property("Producer and Consumer APIs should write and read records") {
    val groupId = genGroupId
    println(s"2 groupId=$groupId")
    val topic = createTopic()

    forAll { values: Vector[(String, String)] =>
      val io = for {
        p <- ProducerApi.create[IO, String, String](BootstrapServers(bootstrapServer))
        c <- ConsumerApi.create[IO, String, String](BootstrapServers(bootstrapServer), GroupId(groupId), AutoOffsetReset.earliest)
        v <- writeAndRead(p, c, topic, values).compile.toVector
      } yield v
      val actual = io.unsafeRunSync()
      actual should === (values)
    }
  }

  property("Write and read in blocking context") {
    val groupId = genGroupId
    println(s"3 groupId=$groupId")
    val topic = createTopic()

    forAll { values: Vector[(Int, String)] =>
      val io = for {
        p <- ProducerApi.createShifting[IO, Int, String](producerContext, BootstrapServers(bootstrapServer))
        c <- ConsumerApi.createShifting[IO, Int, String](BootstrapServers(bootstrapServer), GroupId(groupId), AutoOffsetReset.earliest)
        v <- writeAndRead(p, c, topic, values).compile.toVector
      } yield v
      val actual = io.unsafeRunSync()
      actual should === (values)
    }
  }

  property("read through final offsets") {
    val topic = createTopic(3)

    val data = Map(
      0 -> List("0-0", "0-1"),
      1 -> List("1-0", "1-1"),
      2 -> List("2-0", "2-1"),
    )
    val records = data.toList.flatMap{case (p, vs) => vs.map(v => new ProducerRecord(topic, p, v, v))}
    val tp0 = new TopicPartition(topic, 0)
    val tp1 = new TopicPartition(topic, 1)
    val tp2 = new TopicPartition(topic, 2)

    (for {
      p <- ProducerApi.create[IO, String, String](BootstrapServers(bootstrapServer))
      _ <- p.sendSyncBatch(records)

      //max.poll.records=1 forces stream to repeat a few times, so we validate the takeThrough predicate
      c <- ConsumerApi.create[IO, String, String](BootstrapServers(bootstrapServer), MaxPollRecords(1))
      _ <- c.assign(List(tp0, tp1, tp2))
      _ <- c.seekToBeginning(List(tp0))
      _ <- c.seek(tp1, 1)
      _ <- c.seekToEnd(List(tp2))
      vs <- c.recordsThroughOffsets(Map(tp0 -> 1, tp1 -> 1, tp2 -> 1), 1 second).map(_.asScala.map(_.value)).compile.toList

      //consumer must still be usable after stream halts, positioned immediately after all of the records it's already returned
      _ <- p.sendSync(new ProducerRecord(topic, null, "new"))
      rs <- c.poll(1 second)

      _ <- p.close
      _ <- c.close
    } yield  {
      vs.flatten should contain theSameElementsAs (List("0-0", "0-1", "1-1"))
      rs.asScala.map(_.value) should === (List("new"))
    }).unsafeRunSync()
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
      p <- ProducerApi.create[IO, String, String](BootstrapServers(bootstrapServer))
      _ <- Stream.emits(expected).covary[IO].map(s => new ProducerRecord(topic, s, s)).through(p.sinkWithClose).compile.drain
      consume: Stream[IO, String] = for {
        c <- Stream.eval(ConsumerApi.create[IO, String, String](BootstrapServers(bootstrapServer), GroupId(groupId), AutoOffsetReset.earliest, EnableAutoCommit(false)))
        v <- c.readProcessCommit(c.subscribe(topic), 100 millis)(r => storeOrFail(values, r.value)) //only consumes until a failure
      } yield v
      _ <- consume.attempt.repeat.takeThrough(_ != Right(expected.last)).compile.drain //keeps consuming until last record is successfully processed
      vs <- values.get
    } yield vs
    val actual = io.unsafeRunSync()
    actual should === (expected) //verifies that no successfully processed record was ever reprocessed
  }

  property("Producer transaction works") {
    pending
  }

  property("Avro serdes") {
    val groupId = genGroupId
    println(s"5 groupId=$groupId")
    val topic = createTopic()

    forAll { values: Vector[(String, Person)] =>
      val io = for {
        p <- ProducerApi.avro[IO, String, Person](BootstrapServers(bootstrapServer), SchemaRegistryUrl(schemaRegistryUrl))
        c <- ConsumerApi.avroSpecific[IO, String, Person](BootstrapServers(bootstrapServer), GroupId(groupId), AutoOffsetReset.earliest, SchemaRegistryUrl(schemaRegistryUrl))
        v <- writeAndRead(p, c, topic, values).compile.toVector
      } yield v
      val actual = io.unsafeRunSync()
      actual should === (values)
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
      val io = for {
        p <- ProducerApi.avro4s[IO, PersonId, Person2](BootstrapServers(bootstrapServer), SchemaRegistryUrl(schemaRegistryUrl))
        c <- ConsumerApi.avro4s[IO, PersonId, Person2](BootstrapServers(bootstrapServer), GroupId(groupId), AutoOffsetReset.earliest, SchemaRegistryUrl(schemaRegistryUrl))
        v <- writeAndRead(p, c, topic, values).compile.toVector
      } yield v
      val actual = io.unsafeRunSync()
      actual should === (values)
    }
  }

}
