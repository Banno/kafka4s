# kafka4s - Functional programming with Kafka and Scala

![CI](https://github.com/Banno/kafka4s/workflows/CI/badge.svg) 
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.banno/kafka4s_2.13/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.banno/kafka4s_2.13) 
[![Javadocs](https://www.javadoc.io/badge/com.banno/kafka4s_2.13.svg?color=red&label=scaladoc)](https://www.javadoc.io/doc/com.banno/kafka4s_2.13/latest/com/banno/kafka/index.html)
[![License](http://img.shields.io/:license-Apache%202-green.svg)](http://www.apache.org/licenses/LICENSE-2.0.txt)
![Code of Conduct](https://img.shields.io/badge/Code%20of%20Conduct-Scala-blue.svg)

kafka4s provides pure, referentially transparent functions for working with Kafka, and integrates with FP libraries such as [cats-effect](https://typelevel.org/cats-effect) and [fs2](https://fs2.io).

## [Head on over to the microsite](https://banno.github.io/kafka4s)

## Quick Start

To use kafka4s in an existing SBT project with Scala 2.12 or a later version, add the following dependencies to your
`build.sbt` depending on your needs:

```scala
libraryDependencies ++= Seq(
  "com.banno" %% "kafka4s" % "<version>"
)
```

Note: If your project uses fs2 1.x, you'll want releases from the 2.x series. For fs2 2.x projects, you'll want 3.x series releases.

Sending records to Kafka is an effect. If we wanted to periodically write random integers to a Kafka topic, we could do:

```scala
Stream
  .resource(ProducerApi.resource[F, Int, Int](BootstrapServers(kafkaBootstrapServers)))
  .flatMap { producer =>
    Stream
      .awakeDelay[F](1 second)
      .evalMap { _ =>
        Sync[F].delay(Random.nextInt()).flatMap { i =>
          producer.sendAndForget(new ProducerRecord(topic.name, i, i))
        }
      }
  }
```

Polling Kafka for records is also an effect, and we can obtain a stream of records from a topic. We can print the even random integers from the above topic using:

```scala
Stream.resource(
   ConsumerApi
      .resource[F, Int, Int](
        BootstrapServers(kafkaBootstrapServers),
        GroupId("example3"),
        AutoOffsetReset.earliest,
        EnableAutoCommit(true)
      )
  )
  .evalTap(_.subscribe(topic.name))
  .flatMap(
    _.recordStream(1.second)
      .map(_.value)
      .filter(_ % 2 == 0)
      .evalMap(i => Sync[F].delay(println(i)))
  )
```

## Learning more

To learn more about kafka4s, start with our [Getting Started Guide](https://banno.github.io/kafka4s/docs/), play with some [example apps](https://github.com/Banno/kafka4s/tree/master/examples/src/main/scala), and check out the [kafka4s Scaladoc](https://www.javadoc.io/doc/com.banno/kafka4s_2.12) for more info.

