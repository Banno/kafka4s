package com.banno.kafka.connect

import org.scalatest._
import cats.implicits._
import cats.data.Kleisli
import cats.effect.{Concurrent, ContextShift, IO, Sync}
import scala.concurrent.ExecutionContext
import org.apache.kafka.connect.source.SourceTaskContext
import org.apache.kafka.connect.storage.OffsetStorageReader
import org.apache.kafka.connect.data.Schema
import java.util.{Map => JMap, Collection => JCollection}
import scala.collection.JavaConverters._
import scala.concurrent.duration._

class SourceTaskApiSpec extends FlatSpec with Matchers {

  implicit val contextShiftIO: ContextShift[IO] = IO.contextShift(ExecutionContext.global)

  type P = TestPartition
  type O = TestOffset
  type TC = TestTaskConfigs

  "SourceTaskApi impl" should "load stored offsets and call poll function with correct offsets" in {
    val topic = "test"
    val keySchema = Schema.STRING_SCHEMA
    val valueSchema = Schema.STRING_SCHEMA
    def fp(partition: Int): P = TestPartition(partition)
    def fo(offset: Int): O = TestOffset(offset.toString)
    def record(p: Int, o: Int): SourceRecord[P, O] =
      SourceRecord(
        fp(p),
        fo(o),
        topic,
        keySchema,
        s"$topic-${p}@${o}-key",
        valueSchema,
        s"$topic-${p}@${o}-value"
      )
    val p0 = fp(0)
    val p1 = fp(1)
    val o1 = fo(1)
    val o2 = fo(2)
    val configs = TestTaskConfigs(List(p0, p1), o1, "abc", 123, 1 second)
    val props = MapEncoder[TC].encode(configs)
    val taskContext: SourceTaskContext = new SourceTaskContext {
      override def configs(): JMap[String, String] = props.asJava
      override def offsetStorageReader(): OffsetStorageReader = new OffsetStorageReader {
        override def offset[T](partition: JMap[String, T]): JMap[String, Object] = ???
        override def offsets[T](
            partitions: JCollection[JMap[String, T]]
        ): JMap[JMap[String, T], JMap[String, Object]] =
          Map(
            MapEncoder[P].encode(p1).mapValues(_.asInstanceOf[T]).asJava -> MapEncoder[O]
              .encode(o1)
              .mapValues(_.asInstanceOf[Object])
              .asJava
          ).asJava
      }
    }
    val r02 = record(0, 2)
    val r12 = record(1, 2)
    val r1Expected = List(r02, r12)
    val r03 = record(0, 3)
    val r13 = record(1, 3)
    val r2Expected = List(r03, r13)
    val records: Map[(P, O), List[SourceRecord[P, O]]] = Map(
      (p0, o1) -> List(r02),
      (p1, o1) -> List(r12),
      (p0, o2) -> List(r03),
      (p1, o2) -> List(r13)
    )
    def test[F[_]: Concurrent] =
      for {
        task <- SourceTaskApi[F, P, O, TC](
          "test",
          Kleisli { _: SourceTaskContextApi[F, P, O, TC] =>
            (
              Kleisli { offsets: Map[P, O] =>
                offsets.toList.flatMap(records.get).flatten.pure[F]
              },
              Sync[F].unit
            ).pure[F]
          }.pure[F]
        )
        _ <- task.initialize(taskContext)
        _ <- task.start(props)
        r1 <- task.poll
        r2 <- task.poll
        _ <- task.stop
      } yield {
        r1 should contain theSameElementsAs r1Expected.map(_.toSourceRecord)
        r2 should contain theSameElementsAs r2Expected.map(_.toSourceRecord)
      }
    test[IO].unsafeRunSync()
  }
}
