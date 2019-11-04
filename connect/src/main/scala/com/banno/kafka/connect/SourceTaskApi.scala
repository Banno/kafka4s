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

package com.banno.kafka.connect

import cats.implicits._
import cats.effect.{Concurrent, Sync}
import cats.effect.concurrent.{Deferred, Ref}
import org.apache.kafka.connect.source.{SourceRecord => KCSourceRecord, SourceTaskContext}
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger

trait SourceTaskApi[F[_]] extends TaskApi[F] {
  def commit: F[Unit]
  def commitRecord(record: KCSourceRecord): F[Unit]
  def initialize(context: SourceTaskContext): F[Unit]
  def poll: F[List[KCSourceRecord]]
}

object SourceTaskApi {
  def apply[F[_]: Concurrent, P: MapEncoder: MapDecoder, O: MapDecoder, TC <: TaskConfigs[P, O]: MapDecoder](
      connectorVersion: String,
      initializeSourceTask: F[InitializeSourceTask[F, P, O, TC]]
  ): F[SourceTaskApi[F]] =
    for {
      pollSourceFromOffset <- Deferred[F, PollSourceFromOffset[F, P, O]]
      stopTask <- Deferred[F, StopTask[F]]
      lastOffsets <- Ref.of[F, Map[P, O]](Map.empty)
      log <- Slf4jLogger.create[F]
      task = new SourceTaskApi[F] {

        def initialize(context: SourceTaskContext): F[Unit] =
          for {
            init <- initializeSourceTask
            ctx = SourceTaskContextApi[F, P, O, TC](context)
            configs <- ctx.configs
            storedOffsets <- ctx.offsets(configs.sourcePartitions)
            initialOffsets = configs.sourcePartitions
              .filterNot(storedOffsets.keySet.contains)
              .map(p => (p, configs.initialOffset))
              .toMap
            _ <- if (storedOffsets.nonEmpty)
              log.info(s"Using last committed offsets: $storedOffsets")
            else Sync[F].unit
            _ <- if (initialOffsets.nonEmpty) log.info(s"Using initial offsets: $initialOffsets")
            else Sync[F].unit
            _ <- lastOffsets.set(storedOffsets ++ initialOffsets)
            (p, c) <- init(ctx)
            _ <- pollSourceFromOffset.complete(p)
            _ <- stopTask.complete(c)
          } yield ()

        def poll: F[List[KCSourceRecord]] =
          for {
            p <- pollSourceFromOffset.get
            os <- lastOffsets.get
            records <- p(os)
            _ <- lastOffsets.update(
              _ ++ records.groupBy(_.sourcePartition).mapValues(_.last.sourceOffset) //TODO do something more efficient than groupBy
            )
          } yield records.map(_.toSourceRecord)

        def stop: F[Unit] = stopTask.get.flatten

        def version: F[String] = connectorVersion.pure[F]

        def start(props: Map[String, String]): F[Unit] = Sync[F].unit
        def commit: F[Unit] = Sync[F].unit
        def commitRecord(record: KCSourceRecord): F[Unit] = Sync[F].unit
      }
    } yield task
}
