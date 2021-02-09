package com.banno.kafka

import cats.syntax.all._
import cats.effect._
import com.dimafeng.testcontainers.{
  DockerComposeContainer,
  ExposedService,
  GenericContainer,
  KafkaContainer
}
import org.testcontainers.containers.wait.strategy.Wait

import java.io.File

trait ConfluentContainers[F[_]] {
  def schemaRegistryUrl: F[String]
  def bootstrapServers: F[String]
}
object ConfluentContainers {

  val schemaRegistry = ExposedService("schema-registry", 8091)
  val kafkaBroker = ExposedService("kafka0", 9092)

  def containers[F[_]: Sync]: Resource[F, DockerComposeContainer] =
    Resource.make(
      Sync[F]
        .delay {
          DockerComposeContainer(
            composeFiles = new File("core/src/test/resources/docker-compose.yml"),
            exposedServices = List(schemaRegistry, kafkaBroker),
          )
        }
        .flatTap(a => Sync[F].delay(a.start))
    )(a => Sync[F].delay(a.stop))

  def resource[F[_]: Sync]: Resource[F, ConfluentContainers[F]] =
    containers.map(
      a =>
        new ConfluentContainers[F] {
          def hostAndPort(svc: ExposedService): F[(String, Int)] =
            (
              Sync[F].delay(a.container.getServiceHost(svc.name, svc.port.toInt)),
              Sync[F].delay(a.container.getServicePort(svc.name, svc.port.toInt).toInt)
            ).tupled

          override final def schemaRegistryUrl: F[String] =
            hostAndPort(schemaRegistry).map {
              case (host, port) =>
                s"http://$host:$port"
            }

          override final def bootstrapServers: F[String] =
            hostAndPort(kafkaBroker).map {
              case (host, port) =>
                s"$host:$port"
            }

        }
    )
  def kafka[F[_]: Sync](version: String): Resource[F, KafkaContainer] =
    Resource.make(
      Sync[F]
        .delay(KafkaContainer(confluentPlatformVersion = version))
        .flatTap(a => Sync[F].delay(a.start))
    )(a => Sync[F].delay(a.stop))

  def schemaRegistry[F[_]: Sync](
      version: String,
      kafkaContainer: KafkaContainer,
      internalPort: Int = 8081,
  ): Resource[F, GenericContainer] =
    Resource.make(
      Sync[F]
        .delay {
          GenericContainer(
            dockerImage = s"confluentinc/cp-schema-registry:$version",
            exposedPorts = List(internalPort),
            env = Map(
              "SCHEMA_REGISTRY_HOST_NAME" -> "schema-registry",
              "SCHEMA_REGISTRY_LISTENERS" -> s"http://0.0.0.0:$internalPort",
              "SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS" -> kafkaContainer.container.getBootstrapServers,
            ),
            waitStrategy = Wait.forListeningPort
            //              waitStrategy = Wait.forHttp("/subjects")
          )
        }
        .flatTap(a => Sync[F].delay(a.start))
    )(a => Sync[F].delay(a.stop))
}
