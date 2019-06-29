package example1

import cats.implicits._
import cats.effect._

object CatsEffectApp extends IOApp {
  override def run(args: List[String]): IO[ExitCode] =
    ExampleApp[IO].example.as(ExitCode.Success)
}
