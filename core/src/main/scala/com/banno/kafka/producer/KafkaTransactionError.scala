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

package com.banno.kafka.producer

import cats.implicits._
import cats.ApplicativeError
import org.apache.kafka.common.errors._

/** Represents the types of failures during Kafka transactional writes. */
sealed trait KafkaTransactionError

/** This failure means that the transaction was aborted, and the producer may continue to be used. */
final case class TransactionAborted(error: Throwable)
    extends RuntimeException(error)
    with KafkaTransactionError

/** This failure means that the producer instance can no longer be used, and must be closed (and re-created). */
final case class FatalError(error: Throwable)
    extends RuntimeException(error)
    with KafkaTransactionError

object KafkaTransactionError {
  // Exception-handling is described in the javadocs for the KafkaProducer send and commitTransaction methods.
  def apply[F[_]](e: Throwable, p: ProducerApi[F, _, _])(
      implicit F: ApplicativeError[F, Throwable]
  ): F[Unit] = e match {
    // This fatal exception indicates that another producer with the same transactional.id has been started. When you encounter this exception, you must close the producer instance.
    case e: ProducerFencedException => ApplicativeError[F, Throwable].raiseError(FatalError(e))
    // This exception indicates that the broker received an unexpected sequence number from the producer, which means that data may have been lost. For transactional producers, this is a fatal error and you should close the producer.
    case e: OutOfOrderSequenceException => ApplicativeError[F, Throwable].raiseError(FatalError(e))
    // Indicates that a request API or version needed by the client is not supported by the broker. This is typically a fatal error as Kafka clients will downgrade request versions as needed except in cases where a needed feature is not available in old versions. Fatal errors can generally only be handled by closing the client instance.
    case e: UnsupportedVersionException => ApplicativeError[F, Throwable].raiseError(FatalError(e))
    // In the context of transactions, maybe this is usually TransactionalIdAuthorizationException? At any rate, this is a fatal exception, so production should be closed.
    case e: AuthorizationException => ApplicativeError[F, Throwable].raiseError(FatalError(e))
    // Per docs, on any other type of exception thrown during a transaction write, the tx should be aborted, and the program may continue using the producer.
    case e => p.abortTransaction *> ApplicativeError[F, Throwable].raiseError(TransactionAborted(e))
  }
}
