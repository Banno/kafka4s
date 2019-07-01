---
layout: docs
title: Getting Started
---

# Getting dependency

To use kafka4s in an existing SBT project with Scala 2.12 or a later version, add the following dependencies to your
`build.sbt` depending on your needs:

```scala
libraryDependencies ++= Seq(
  "com.banno" %% "kafka4s" % "<version>"
)
```

# Some quick examples

First, some initial imports:
```
import cats._, cats.effect._, cats.implicits._, scala.concurrent.duration._
```

### Define our data

We'll define a toy message type for data we want to store in our Kafka topic.

```
case class Customer(name: String, address: String)
case class CustomerId(id: String)
```

### Create our Kafka topic

Now we'll tell Kafka to create a topic that we'll write our Kafka records to.

First, let's bring some types and implicits into scope:

```
import com.banno.kafka._, com.banno.kafka.admin._
import org.apache.kafka.clients.admin.NewTopic
```

Now we can create a topic named `customers.v1` with 1 partition and 3 replicas:

```
val topic = new NewTopic("customers.v1", 1, 3)
val kafkaBootstrapServers = "kafka.local:9092,kafka.local:9093" // Change as needed
AdminApi.createTopicsIdempotent[IO](kafkaBootstrapServers, topic :: Nil).unsafeRunSync
```

### Register our topic schema 

Let's register a schema for our topic with the schema registry!

First, we bring types and implicits into scope:

```
import com.banno.kafka.schemaregistry._
```
Now we initialize a schema registry client:

```
val schemaRegistryUri = "http://kafka.local:8081" // Change as needed
val cachedSchemasPerSubject = 1000
val schemaRegistry = SchemaRegistryApi[IO](schemaRegistryUri, cachedSchemasPerSubject).unsafeRunSync
```

We'll use the name of the topic we created above:

```
val topicName = topic.name
```

Now we can register our topic key and topic value schemas:


```
(for {
  _ <- schemaRegistry.registerKey[CustomerId](topicName)
  _ <- schemaRegistry.registerValue[Customer](topicName)
} yield ()).unsafeRunSync
```

### Write our records to Kafka

Now let's create a producer and send some records to our Kafka topic!

We first bring our Kafka producer utils into scope:

```
import com.banno.kafka.producer._
```

Now we can create our producer instance:

```
val producer = ProducerApi.generic[IO](
  BootstrapServers(kafkaBootstrapServers),
  SchemaRegistryUrl(schemaRegistryUri),
  ClientId("producer-example")
).unsafeRunSync
```

And we'll define some customer records to be written:

```
import org.apache.kafka.clients.producer.ProducerRecord
val recordsToBeWritten = (1 to 10).map(a => new ProducerRecord(topicName, CustomerId(a.toString), Customer(s"name-${a}", s"address-${a}"))).toVector
```

And now we can (attempt to) write our records to Kafka:

```
recordsToBeWritten.traverse_(producer.sendSync)
```

The above fails to compile, however! Our producer writes generic
`ProducerRecord`s, but we'd like to send typed records, to ensure that
our `CustomerId` key and our `Customer` value are compatible with our
topic. For this, we can use Kafka4s' `avro4s` integration!

#### Writing typed records with an Avro4s producer

Turning a generic producer into a typed producer is as simple as the following:

```
val avro4sProducer = producer.toAvro4s[CustomerId, Customer]
```

We can now write our typed customer records successfully!

```
recordsToBeWritten.traverse_(avro4sProducer.sendSync).unsafeRunSync
```

### Read our records from Kafka

Now that we've stored some records in Kafka, let's read them as an `fs2.Stream`!

We first import our Kafka consumer utilities:
```
import com.banno.kafka.consumer._
```

Now we can create our consumer instance.

We'll create a "shifting" Avro4s consumer, which will shift its blocking calls to a dedicated `ExecutionContext`, to avoid blocking the main work pool's (typically `ExecutionContext.global`) threads. After receiving records, work is then shifted back to the work pool. We'll want an implicit `ContextShift` instance in scope to manage this thread shifting for us.

Here's our `ContextShift`:

```
implicit val CS = IO.contextShift(scala.concurrent.ExecutionContext.global)
```

And here's our consumer, along with the `ExecutionContext` we'll want our consumer to use:

```
val blockingContext = ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(1)) 
val consumer = ConsumerApi.avro4sShifting[IO, CustomerId, Customer](
  blockingContext,
  BootstrapServers(kafkaBootstrapServers), 
  SchemaRegistryUrl(schemaRegistryUri),
  ClientId("consumer-example"),
  GroupId("consumer-example-group")
).unsafeRunSync
```

With our Kafka consumer in hand, we can now read a stream of messages from Kafka:

```
val messageStream = consumer.recordStream(
  initialize = consumer.subscribe(topicName),
  pollTimeout = 1.second
)
```

And we can now run the stream to retrieve the topic's messages:

```
val messages = messageStream.take(5).compile.toVector.unsafeRunSync
```

Voila!

Now that we've seen a quick overview, we can take a look at more in-depth documentation of Kafka4s utilities.
