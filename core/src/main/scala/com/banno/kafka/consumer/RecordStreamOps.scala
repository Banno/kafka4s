package com.banno.kafka.consumer

import fs2.Stream
import org.apache.kafka.common.errors.WakeupException

case class RecordStreamOps[F[_], A](s: Stream[F, A]) {

  def haltOnWakeup: Stream[F, A] =
    s.handleErrorWith {
      case _: WakeupException => Stream.empty
    }
}
