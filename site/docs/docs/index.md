---
layout: docs
title: Getting Started
---

# Getting dependency

To use kafka4s in an existing SBT project with Scala 2.13 or a later version, add the following dependencies to your
`build.sbt` depending on your needs:

```scala
libraryDependencies ++= Seq(
  "com.banno" %% "kafka4s" % "<version>"
)
```

# Some quick examples

First, some initial imports:
```scala mdoc
import cats._, cats.effect._, cats.implicits._, scala.concurrent.duration._
```

### Define our data

We'll define a toy message type for data we want to store in our Kafka topic.

```scala mdoc
case class Customer(name: String, address: String)
case class CustomerId(id: String)
```

### Create our Kafka topic

Now we'll tell Kafka to create a topic that we'll write our Kafka records to.

First, let's bring some types and implicits into scope:

```scala mdoc
import com.banno.kafka._, com.banno.kafka.admin._
import org.apache.kafka.clients.admin.NewTopic
```

Now we can create a topic named `customers.v1` with 1 partition and 1 replica:

```scala mdoc
val topic = new NewTopic("customers.v1", 1, 1.toShort)
val kafkaBootstrapServers = "localhost:9092" // Change as needed
```

```scala mdoc:compile-only
import cats.effect.unsafe.implicits.global
AdminApi.createTopicsIdempotent[IO](kafkaBootstrapServers, topic :: Nil).unsafeRunSync()
```

### Register our topic schema

Let's register a schema for our topic with the schema registry!

First, we bring types and implicits into scope:

```scala mdoc
import com.banno.kafka.schemaregistry._
```

We'll use the name of the topic we created above:

```scala mdoc
val topicName = topic.name
```

Now we can register our topic key and topic value schemas:

```scala mdoc
val schemaRegistryUri = "http://localhost:8091" // Change as needed
```

```scala mdoc:compile-only
import cats.effect.unsafe.implicits.global
SchemaRegistryApi.register[IO, CustomerId, Customer](
  schemaRegistryUri, topicName
).unsafeRunSync()
```

### Write our records to Kafka

Now let's create a producer and send some records to our Kafka topic!

We first bring our Kafka producer utils into scope:

```scala mdoc
import com.banno.kafka.producer._
```

Now we can create our producer instance:

```scala mdoc
val producer = ProducerApi.Avro.Generic.resource[IO](
  BootstrapServers(kafkaBootstrapServers),
  SchemaRegistryUrl(schemaRegistryUri),
  ClientId("producer-example")
)
```

And we'll define some customer records to be written:

```scala mdoc
import org.apache.kafka.clients.producer.ProducerRecord
val recordsToBeWritten = (1 to 10).map(a => new ProducerRecord(topicName, CustomerId(a.toString), Customer(s"name-${a}", s"address-${a}"))).toVector
```

And now we can (attempt to) write our records to Kafka:

```scala mdoc:fail
producer.use(p => recordsToBeWritten.traverse_(p.sendSync))
```

The above fails to compile, however! Our producer writes generic
`ProducerRecord`s, but we'd like to send typed records, to ensure that
our `CustomerId` key and our `Customer` value are compatible with our
topic. For this, we can use Kafka4s' `avro4s` integration!

#### Writing typed records with an Avro4s producer

Turning a generic producer into a typed producer is simple. We first ensure that `com.sksamuel.avro4s.RecordFormat` instances for our data are in scope:

```scala mdoc
implicit val CustomerRecordFormat = com.sksamuel.avro4s.RecordFormat[Customer]
implicit val CustomerIdRecordFormat = com.sksamuel.avro4s.RecordFormat[CustomerId]

```

And with those implicits in scope, we can create our producer:

```scala mdoc
val avro4sProducer = producer.map(_.toAvro4s[CustomerId, Customer])
```

We can now write our typed customer records successfully!

```scala mdoc:compile-only
import cats.effect.unsafe.implicits.global
avro4sProducer.use(p =>
  recordsToBeWritten.traverse_(r => p.sendSync(r).flatMap(rmd => IO(println(s"Wrote record to ${rmd}"))))
).unsafeRunSync()
```

### Read our records from Kafka

Now that we've stored some records in Kafka, let's read them as an `fs2.Stream`!

We first import our Kafka consumer utilities:
```scala mdoc
import com.banno.kafka.consumer._
```

Now we can create our consumer instance.

**TODO**: Rethink for CE3

By default, kafka4s consumers shift blocking calls to a dedicated `ExecutionContext` backed by a singleton thread pool, to avoid blocking the main work pool's (typically `ExecutionContext.global`) threads, and as a simple synchronization mechanism because the underlying Java client `KafkaConsumer` is not thread-safe. After receiving records, work is then shifted back to the work pool. We'll want an implicit `ContextShift` instance in scope to manage this thread shifting for us.

Here's our `ContextShift`:

```scala mdoc
//import scala.concurrent.ExecutionContext
//implicit val CS = IO.contextShift(ExecutionContext.global)
```

And here's our consumer, which is using Avro4s to deserialize the records:

```scala mdoc
val consumer = ConsumerApi.Avro4s.resource[IO, CustomerId, Customer](
  BootstrapServers(kafkaBootstrapServers),
  SchemaRegistryUrl(schemaRegistryUri),
  ClientId("consumer-example"),
  GroupId("consumer-example-group")
)
```

With our Kafka consumer in hand, we'll assign to our consumer our topic partition, with no offsets, so that it starts reading from the first record, and read a stream of records from our Kafka topic:
```scala mdoc
import org.apache.kafka.common.TopicPartition
val initialOffsets = Map.empty[TopicPartition, Long] // Start from beginning
```

```scala mdoc:compile-only
import cats.effect.unsafe.implicits.global
val messages = consumer.use(c =>
  c.assign(topicName, initialOffsets) *> c.recordStream(1.second).take(5).compile.toVector
).unsafeRunSync()
```

Because the producer and consumer above were created within a `Resource` context, everything was closed and shut down properly.

Now that we've seen a quick overview, we can take a look at more in-depth documentation of Kafka4s utilities.
