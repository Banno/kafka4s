---
layout: home

---

# kafka4s - Functional programming with Kafka and Scala

kafka4s provides pure, referentially transparent functions for working with Kafka, and integrates with FP libraries such as [cats-effect](https://typelevel.org/cats-effect) and [fs2](https://fs2.io).

## Quick Start

To use kafka4s in an existing SBT project with Scala 2.12 or a later version, add the following dependencies to your
`build.sbt` depending on your needs:

```scala
resolvers += "confluent" at "https://packages.confluent.io/maven/"

libraryDependencies ++= Seq(
  "com.banno" %% "kafka4s" % "<version>"
)
```

Sending records to Kafka is an effect. If we wanted to periodically write random integers to a Kafka topic, we could do:

```scala
Stream.resource(
ProducerApi
  .resource[F, Int, Int](BootstrapServers(kafkaBootstrapServers))
  .map(
    p =>
      Timer[F]
        .sleep(1 second)
        .flatMap(
          _ =>
            Sync[F]
              .delay(Random.nextInt())
              .flatMap(i => p.sendAndForget(new ProducerRecord(topic.name, i, i)))
        )
  )
)
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

To learn more about kafka4s, start with our [Getting Started Guide](/kafka4s/docs/), play with some [example apps](https://github.com/Banno/kafka4s/tree/master/examples/src/main/scala), and check out the [kafka4s Scaladoc](https://www.javadoc.io/doc/com.banno/kafka4s_2.12) for more info.
