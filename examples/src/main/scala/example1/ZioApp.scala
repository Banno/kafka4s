package example1

import zio._
import zio.interop.catz._

object ZioApp extends CatsApp {
  override def run(args: List[String]): ZIO[ZioApp.Environment, Nothing, Int] =
    ExampleApp[Task].example.fold(_ => 1, _ => 0)
}
