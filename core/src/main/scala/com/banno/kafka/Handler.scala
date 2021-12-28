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

package com.banno.kafka

import cats._
import cats.syntax.all._
import shapeless._

object Handler {
  private def nil[F[_]]: CNil => F[Unit] =
    _.impossible

  private def cons[F[_], A, B <: Coproduct](
      left: A => F[Unit],
      right: B => F[Unit],
  ): A :+: B => F[Unit] =
    _.eliminate(left, right)

  def double[F[_]: Applicative, A](
      f: A => F[Unit],
      g: A => F[Unit],
  ): A => F[Unit] = x => f(x) *> g(x)

  final case class Builder[F[_], A <: Coproduct] private[Handler] (
      private val build: (CNil => F[Unit]) => A => F[Unit]
  ) {
    def and[B](p: B => F[Unit]): Builder[F, B :+: A] =
      Builder(n => cons(p, build(n)))

    def finis: A => F[Unit] = build(nil)
  }

  object Batched {
    private def partition[F[_]: Monad, A, B <: Coproduct](
        left: List[A] => F[Unit],
        right: List[B] => F[Unit],
    ): List[A :+: B] => F[Unit] =
      xs => {
        val (ls, rs) = xs.partitionMap(_.eliminate(_.asLeft, _.asRight))
        left(ls) *> right(rs)
      }

    final case class Builder[F[_]: Monad, A <: Coproduct] private[Batched] (
        private val build: List[A] => F[Unit]
    ) {
      def and[B](p: List[B] => F[Unit]): Builder[F, B :+: A] =
        Builder(partition(p, build))

      def finis: List[A] => F[Unit] = build
    }

    def combine[F[_]: Monad, A](
        leftmost: List[A] => F[Unit]
    ): Builder[F, A :+: CNil] =
      Builder(xs => leftmost(xs.map(_.eliminate(identity, _.impossible))))
  }

  def combine[F[_], A](leftmost: A => F[Unit]): Builder[F, A :+: CNil] =
    Builder(cons(leftmost, _))
}

// If this were a typeclass, it would be an alleycat: it's Comonad sans
// CoflatMap. For our purposes, though, it just makes up for the lack of
// higher-rank quantification in Scala 2; we won't be passing it implicitly, so
// its lack of laws shouldn't matter.
trait Extractor[F[_]] {
  import Extractor.Composed

  def extract[A](fa: F[A]): A
  def andThen[G[_]](e: Extractor[G]): Extractor[Composed[F, G, *]] = {
    val self = this
    new Extractor[Composed[F, G, *]] {
      override def extract[A](fa: F[G[A]]): A =
        e.extract(self.extract(fa))
    }
  }
}

object Extractor {
  type Composed[F[_], G[_], A] = F[G[A]]
}

object Adapt {
  def apply[F[_], K, V, W[_]](
      handle: Product2[K, V] => F[Unit],
      extractor: Extractor[W],
  ): IncomingRecord[K, W[V]] => F[Unit] =
    handle.compose(_.map(extractor.extract))

  def apply[F[_], K, V, W](
      handle: Product2[K, V] => F[Unit],
      adapter: W => V,
  ): Product2[K, W] => F[Unit] =
    handle.compose(kv => (kv._1, adapter(kv._2)))
}
