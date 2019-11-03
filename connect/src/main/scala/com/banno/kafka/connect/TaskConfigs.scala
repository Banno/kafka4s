package com.banno.kafka.connect

trait TaskConfigs[P, O] {
  def sourcePartitions: List[P]
  def initialOffset: O
}
