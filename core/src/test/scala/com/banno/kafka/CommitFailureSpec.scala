/*
 * Copyright 2019 Jack Henry & Associates, Inc.®
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.banno.kafka

import java.util.concurrent.CountDownLatch
import java.util.{Map => JMap}

import cats.effect.{IO, Ref}
import fs2.{CompositeFailure, Stream}
import munit.CatsEffectSuite
import org.apache.kafka.clients.consumer.{MockConsumer, OffsetAndMetadata}
import org.apache.kafka.common.TopicPartition
import com.banno.kafka.consumer.*

import scala.concurrent.duration.*

class CommitFailureSpec extends CatsEffectSuite {
  override val munitIOTimeout = 30.seconds

  case class CommitFailure() extends RuntimeException("commit failed")

  case class ProcessFailure() extends RuntimeException("processing failed")

  def flatten(t: Throwable): List[Throwable] =
    t match {
      case cf: CompositeFailure => cf.all.toList.flatMap(flatten)
      case other => List(other)
    }

  /** A consumer which signals attempts commit offsets and always fails. */
  class FailingCommitConsumer(commitAttempted: CountDownLatch)
      extends MockConsumer[Int, Int]("earliest") {
    override def commitSync(
        offsets: JMap[TopicPartition, OffsetAndMetadata]
    ): Unit = {
      commitAttempted.countDown()
      throw CommitFailure()
    }
  }

  val topicPartition = new TopicPartition("test-topic", 0)

  val values: List[Int] = (0 to 9).toList

  def processWithFailingCommits(
      commitAttempted: CountDownLatch,
      endless: Boolean,
  )(process: Int => IO[Int]): Stream[IO, Int] = {
    val consumer =
      ConsumerImpl.create[IO, Int, Int](
        new FailingCommitConsumer(commitAttempted)
      )

    val source =
      if (endless) Stream.emits[IO, Int](values) ++ Stream.never[IO]
      else Stream.emits[IO, Int](values)

    ConsumerOps(consumer).processingAndCommittingAux[Int, Int](
      maxRecordCount = 1,
      maxElapsedTime = Long.MaxValue.nanos,
      keepAliveInterval = Duration.Inf,
    )(
      source,
      process,
      v => Map(topicPartition -> v.toLong),
      _ => 1L,
    )
  }

  test("commit failure is raised to the caller, and stops processing") {
    val commitAttempted = new CountDownLatch(1)

    // The first record is processed normally; subsequent records wait for a
    // commit to be attempted and fail.
    def process(processed: Ref[IO, List[Int]])(v: Int): IO[Int] =
      (if (v == values.head) IO.unit
       else
         IO.interruptible(commitAttempted.await()) *> IO.sleep(100.millis)) *>
      processed.update(_ :+ v).as(v)

    for {
      processed <- Ref[IO].of(List.empty[Int])
      result <- processWithFailingCommits(commitAttempted, endless = true)(
        process(processed)
      )
        .interruptAfter(5.seconds)
        .compile
        .toList
        .attempt
      seen <- processed.get
    } yield {
      assertEquals(
        result,
        Left(CommitFailure()),
        s"the commit fiber died on the first commit, but the stream survived it, " +
        s"processing $seen with nothing committed, and reported success",
      )
      assert(
        seen.size < values.size,
        s"processing continued to $seen after commits started failing",
      )
    }
  }

  test("a failing final flush does not mask a processing failure") {
    val commitAttempted = new CountDownLatch(1)
    val consumer =
      ConsumerImpl.create[IO, Int, Int](
        new FailingCommitConsumer(commitAttempted)
      )

    // Nothing is committed until the stream terminates, so the only commit
    // attempted is the final flush, after processing has already failed.
    val stream = ConsumerOps(consumer).processingAndCommittingAux[Int, Int](
      maxRecordCount = Long.MaxValue,
      maxElapsedTime = Long.MaxValue.nanos,
      keepAliveInterval = Duration.Inf,
    )(
      Stream.emits[IO, Int](values),
      v => if (v == 7) IO.raiseError(ProcessFailure()) else IO.pure(v),
      v => Map(topicPartition -> v.toLong),
      _ => 1L,
    )

    stream.compile.toList.attempt.map { result =>
      val raised = result.swap.toOption.toList.flatMap(flatten)
      assert(
        raised.contains(ProcessFailure()),
        s"the processing failure was lost, leaving only $raised",
      )
    }
  }

  test("commit failure during the final flush is raised to the caller") {
    val commitAttempted = new CountDownLatch(1)
    processWithFailingCommits(commitAttempted, endless = false)(
      IO.pure
    ).compile.toList.attempt
      .map { result =>
        assertEquals(
          result,
          Left(CommitFailure()),
          "the stream finished and reported success, even though the offsets " +
          "it had just processed could not be committed",
        )
      }
  }
}
