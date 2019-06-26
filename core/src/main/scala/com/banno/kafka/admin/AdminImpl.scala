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

import cats.effect.Sync
import org.apache.kafka.common.TopicPartitionReplica
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.acl.{AclBinding, AclBindingFilter}
import org.apache.kafka.clients.admin._
import scala.concurrent.duration._
import scala.collection.JavaConverters._

case class AdminImpl[F[_]](a: AdminClient)(implicit F: Sync[F]) extends AdminApi[F] {
  def alterConfigs(configs: Map[ConfigResource, Config]): F[AlterConfigsResult] =
    F.delay(a.alterConfigs(configs.asJava))
  def alterConfigs(
      configs: Map[ConfigResource, Config],
      options: AlterConfigsOptions
  ): F[AlterConfigsResult] =
    F.delay(a.alterConfigs(configs.asJava, options))
  def alterReplicaLogDirs(
      replicaAssignment: Map[TopicPartitionReplica, String]
  ): F[AlterReplicaLogDirsResult] =
    F.delay(a.alterReplicaLogDirs(replicaAssignment.asJava))
  def alterReplicaLogDirs(
      replicaAssignment: Map[TopicPartitionReplica, String],
      options: AlterReplicaLogDirsOptions
  ): F[AlterReplicaLogDirsResult] =
    F.delay(a.alterReplicaLogDirs(replicaAssignment.asJava, options))
  def close: F[Unit] = F.delay(a.close())
  def close(timeout: FiniteDuration): F[Unit] =
    F.delay(a.close(java.time.Duration.ofMillis(timeout.toMillis)))
  def createAcls(acls: Iterable[AclBinding]): F[CreateAclsResult] =
    F.delay(a.createAcls(acls.asJavaCollection))
  def createAcls(acls: Iterable[AclBinding], options: CreateAclsOptions): F[CreateAclsResult] =
    F.delay(a.createAcls(acls.asJavaCollection, options))
  def createPartitions(newPartitions: Map[String, NewPartitions]): F[CreatePartitionsResult] =
    F.delay(a.createPartitions(newPartitions.asJava))
  def createPartitions(
      newPartitions: Map[String, NewPartitions],
      options: CreatePartitionsOptions
  ): F[CreatePartitionsResult] =
    F.delay(a.createPartitions(newPartitions.asJava, options))
  def createTopics(newTopics: Iterable[NewTopic]): F[CreateTopicsResult] =
    F.delay(a.createTopics(newTopics.asJavaCollection))
  def createTopics(
      newTopics: Iterable[NewTopic],
      options: CreateTopicsOptions
  ): F[CreateTopicsResult] =
    F.delay(a.createTopics(newTopics.asJavaCollection, options))
  def deleteAcls(filters: Iterable[AclBindingFilter]): F[DeleteAclsResult] =
    F.delay(a.deleteAcls(filters.asJavaCollection))
  def deleteAcls(
      filters: Iterable[AclBindingFilter],
      options: DeleteAclsOptions
  ): F[DeleteAclsResult] =
    F.delay(a.deleteAcls(filters.asJavaCollection, options))
  def deleteTopics(topics: Iterable[String]): F[DeleteTopicsResult] =
    F.delay(a.deleteTopics(topics.asJavaCollection))
  def deleteTopics(topics: Iterable[String], options: DeleteTopicsOptions): F[DeleteTopicsResult] =
    F.delay(a.deleteTopics(topics.asJavaCollection, options))
  def describeAcls(filter: AclBindingFilter): F[DescribeAclsResult] =
    F.delay(a.describeAcls(filter))
  def describeAcls(filter: AclBindingFilter, options: DescribeAclsOptions): F[DescribeAclsResult] =
    F.delay(a.describeAcls(filter, options))
  def describeCluster: F[DescribeClusterResult] = F.delay(a.describeCluster())
  def describeCluster(options: DescribeClusterOptions): F[DescribeClusterResult] =
    F.delay(a.describeCluster(options))
  def describeConfigs(resources: Iterable[ConfigResource]): F[DescribeConfigsResult] =
    F.delay(a.describeConfigs(resources.asJavaCollection))
  def describeConfigs(
      resources: Iterable[ConfigResource],
      options: DescribeConfigsOptions
  ): F[DescribeConfigsResult] =
    F.delay(a.describeConfigs(resources.asJavaCollection, options))
  def describeLogDirs(brokers: Iterable[Int]): F[DescribeLogDirsResult] =
    F.delay(a.describeLogDirs(brokers.map(Int.box).asJavaCollection))
  def describeLogDirs(
      brokers: Iterable[Int],
      options: DescribeLogDirsOptions
  ): F[DescribeLogDirsResult] =
    F.delay(a.describeLogDirs(brokers.map(Int.box).asJavaCollection, options))
  def describeReplicaLogDirs(
      replicas: Iterable[TopicPartitionReplica]
  ): F[DescribeReplicaLogDirsResult] =
    F.delay(a.describeReplicaLogDirs(replicas.asJavaCollection))
  def describeReplicaLogDirs(
      replicas: Iterable[TopicPartitionReplica],
      options: DescribeReplicaLogDirsOptions
  ): F[DescribeReplicaLogDirsResult] =
    F.delay(a.describeReplicaLogDirs(replicas.asJavaCollection, options))
  def describeTopics(topicNames: Iterable[String]): F[DescribeTopicsResult] =
    F.delay(a.describeTopics(topicNames.asJavaCollection))
  def describeTopics(
      topicNames: Iterable[String],
      options: DescribeTopicsOptions
  ): F[DescribeTopicsResult] =
    F.delay(a.describeTopics(topicNames.asJavaCollection, options))
  def listTopics: F[ListTopicsResult] = F.delay(a.listTopics())
  def listTopics(options: ListTopicsOptions): F[ListTopicsResult] = F.delay(a.listTopics(options))
}
