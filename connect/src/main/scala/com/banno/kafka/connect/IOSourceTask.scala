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

import org.apache.kafka.connect.source.{
  SourceRecord => KCSourceRecord,
  SourceTask,
  SourceTaskContext
}
import cats.effect.IO
import java.util.{List => JList, Map => JMap}
import scala.collection.JavaConverters._

/** Source connectors should provide a class that extends this, and return it from the connector's taskClass method. Kafka Connect will instantiate it via reflection, to run the tasks.
  * The "end of the universe" for source tasks. */
abstract class IOSourceTask(apiIO: IO[SourceTaskApi[IO]]) extends SourceTask {
  val api: SourceTaskApi[IO] = apiIO.unsafeRunSync()
  override def commit(): Unit =
    api.commit.unsafeRunSync()
  override def commitRecord(record: KCSourceRecord): Unit =
    api.commitRecord(record).unsafeRunSync()
  override def initialize(context: SourceTaskContext): Unit =
    api.initialize(context).unsafeRunSync()
  override def poll(): JList[KCSourceRecord] =
    api.poll.map(_.asJava).unsafeRunSync()
  override def start(props: JMap[String, String]): Unit =
    api.start(props.asScala.toMap).unsafeRunSync()
  override def stop(): Unit =
    api.stop.unsafeRunSync()
  override def version(): String =
    api.version.unsafeRunSync()
}
