---
layout: docs
title: Kafka Connect
---

# Writing Connectors

The `kafka4s-connect` module can help you write Kafka Connect source and sink connectors using pure FP in Scala. The following resources are helpful to learn about writing connectors in general: [Apache Kafka docs](http://kafka.apache.org/documentation/#connect_development), and [Confluent docs](https://docs.confluent.io/current/connect/devguide.html).

To get started, include the `kafka-connect` dependency in your sbt project:

```scala
libraryDependencies ++= Seq(
  "com.banno" %% "kafka4s-connect" % "<version>"
)
```

## Source Connectors

- extend IOSourceConnector and give it a ConnectorApi[F] impl

Kafka4s provides the `ConnectorApi[F[_]]` trait, which you need to provide an instance of. For example:

```scala
object ExampleSourceConnectorApi {
  def apply[F[_]]: F[ConnectorApi[F]] = ???
}
```

You can implement this trait directly, or use the utilities described below.

Kafka Connect runs a connector by instantiating the connector class via Java reflection. This is the fully-qualified class name specified in the connector's `connector.class` config value. Define this class similar to:

```scala
class ExampleSourceConnector extends IOSourceConnector(ExampleSourceConnectorApi[IO])
```

Then, in the connector's JSON configs, you will specify `connector.class = com.example.ExampleSourceConnector`.

### Connector Utilities

Many connectors simply determine the number of tasks that should run, and then generate configs for those tasks.

- ConnectorApi companion object apply can help create a ConnectorApi, from a simple function
- SingleTaskConnectorApi and MaxTasksConnectorApi can help in certain simple cases

- extend IOSourceTask and give it a SourceTaskApi[F] impl
- SourceTaskApi companion object apply can help create a SourceTaskApi, from a simple function

## Sink Connectors
