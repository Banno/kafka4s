package com.banno.kafka.connect

trait TaskApi[F[_]] {
  def start(props: Map[String, String]): F[Unit]
  def stop: F[Unit]
  def version: F[String]
}
