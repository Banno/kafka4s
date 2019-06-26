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

package com.banno.kafka.admin

import cats.implicits._
import cats.effect.{Bracket, Sync}
import org.apache.kafka.common.TopicPartitionReplica
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.acl.{AclBinding, AclBindingFilter}
import org.apache.kafka.clients.admin._
import java.util.Properties

import scala.concurrent.duration._
import scala.collection.JavaConverters._
import com.banno.kafka._

/*
All of these FooResult classes have accessor methods that return KafkaFuture[A]
A simple way to handle this is to just wrap the kf.get() call in F.delay, e.g.:
for {
  result <- admin.operation
  value <- F.delay(result.value().get())
  //use value...
}

In the future maybe there's a nicer way to handle KafkaFuture, but for now this works and is still safe.
 */

trait AdminApi[F[_]] {
  def alterConfigs(configs: Map[ConfigResource, Config]): F[AlterConfigsResult]
  def alterConfigs(
      configs: Map[ConfigResource, Config],
      options: AlterConfigsOptions): F[AlterConfigsResult]
  def alterReplicaLogDirs(
      replicaAssignment: Map[TopicPartitionReplica, String]): F[AlterReplicaLogDirsResult]
  def alterReplicaLogDirs(
      replicaAssignment: Map[TopicPartitionReplica, String],
      options: AlterReplicaLogDirsOptions): F[AlterReplicaLogDirsResult]
  def close: F[Unit]
  def close(duration: FiniteDuration): F[Unit]
  def createAcls(acls: Iterable[AclBinding]): F[CreateAclsResult]
  def createAcls(acls: Iterable[AclBinding], options: CreateAclsOptions): F[CreateAclsResult]
  def createPartitions(newPartitions: Map[String, NewPartitions]): F[CreatePartitionsResult]
  def createPartitions(
      newPartitions: Map[String, NewPartitions],
      options: CreatePartitionsOptions): F[CreatePartitionsResult]
  def createTopics(newTopics: Iterable[NewTopic]): F[CreateTopicsResult]
  def createTopics(
      newTopics: Iterable[NewTopic],
      options: CreateTopicsOptions): F[CreateTopicsResult]
  def deleteAcls(filters: Iterable[AclBindingFilter]): F[DeleteAclsResult]
  def deleteAcls(
      filters: Iterable[AclBindingFilter],
      options: DeleteAclsOptions): F[DeleteAclsResult]
  def deleteTopics(topics: Iterable[String]): F[DeleteTopicsResult]
  def deleteTopics(topics: Iterable[String], options: DeleteTopicsOptions): F[DeleteTopicsResult]
  def describeAcls(filter: AclBindingFilter): F[DescribeAclsResult]
  def describeAcls(filter: AclBindingFilter, options: DescribeAclsOptions): F[DescribeAclsResult]
  def describeCluster: F[DescribeClusterResult]
  def describeCluster(options: DescribeClusterOptions): F[DescribeClusterResult]
  def describeConfigs(resources: Iterable[ConfigResource]): F[DescribeConfigsResult]
  def describeConfigs(
      resources: Iterable[ConfigResource],
      options: DescribeConfigsOptions): F[DescribeConfigsResult]
  def describeLogDirs(brokers: Iterable[Int]): F[DescribeLogDirsResult]
  def describeLogDirs(
      brokers: Iterable[Int],
      options: DescribeLogDirsOptions): F[DescribeLogDirsResult]
  def describeReplicaLogDirs(
      replicas: Iterable[TopicPartitionReplica]): F[DescribeReplicaLogDirsResult]
  def describeReplicaLogDirs(
      replicas: Iterable[TopicPartitionReplica],
      options: DescribeReplicaLogDirsOptions): F[DescribeReplicaLogDirsResult]
  def describeTopics(topicNames: Iterable[String]): F[DescribeTopicsResult]
  def describeTopics(
      topicNames: Iterable[String],
      options: DescribeTopicsOptions): F[DescribeTopicsResult]
  def listTopics: F[ListTopicsResult]
  def listTopics(options: ListTopicsOptions): F[ListTopicsResult]
}

object AdminApi {

  def createClient[F[_]: Sync](configs: Map[String, AnyRef]): F[AdminClient] =
    Sync[F].delay(AdminClient.create(configs.asJava))
  def createClient[F[_]: Sync](configs: Properties): F[AdminClient] =
    Sync[F].delay(AdminClient.create(configs))
  def createClient[F[_]: Sync](configs: (String, AnyRef)*): F[AdminClient] =
    createClient(configs.toMap)

  def apply[F[_]: Sync](configs: Map[String, AnyRef]): F[AdminApi[F]] =
    createClient[F](configs).map(AdminImpl[F](_))
  def apply[F[_]: Sync](configs: Properties): F[AdminApi[F]] =
    createClient[F](configs).map(AdminImpl[F](_))
  def apply[F[_]: Sync](configs: (String, AnyRef)*): F[AdminApi[F]] = apply[F](configs.toMap)

  def createTopicsIdempotent[F[_]: Sync: Bracket[?[_], Throwable]](
      bootstrapServers: String,
      topics: Iterable[NewTopic]): F[CreateTopicsResult] =
    Bracket[F, Throwable].bracket(AdminApi[F](BootstrapServers(bootstrapServers)))(a =>
      a.createTopicsIdempotent(topics))(_.close)
}
