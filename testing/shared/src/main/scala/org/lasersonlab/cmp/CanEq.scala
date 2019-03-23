package org.lasersonlab.cmp

import cats.syntax.all._
import cats.{ Applicative, Monad }
import hammerlab.option._
import org.hammerlab.cmp
import org.lasersonlab.cmp.CanEq.Aux

trait CanEq[F[_], L, R] {
  type Δ
  type Result = F[?[Δ]]
  def apply(l: L, r: R): F[?[Δ]]

  def map[L1, R1](
    implicit
    fl: L1 ⇒ L,
    fr: R1 ⇒ R
  ):
      Aux[F, L1, R1, Δ] =
    CanEq[F, L1, R1, Δ] {
      (l, r) ⇒ this(fl(l), fr(r))
    }
  def map[T](
    f: T ⇒ L
  )(
    implicit
    ev: L =:= R
  ):
      Aux[F, T, T, Δ] =
    CanEq[F, T, T, Δ] {
      (l, r) ⇒ this(f(l), f(r))
    }
}

trait Top {
  type Aux[F[_], L, R, D] = CanEq[F, L, R] { type Δ = D }

  def apply[F[_], L, R, D](f: (L, R) ⇒ F[?[D]]): Aux[F, L, R, D] =
    new CanEq[F, L, R] {
      type Δ = D
      def apply(l: L, r: R): Result = f(l, r)
    }
}

  trait WithConversion
extends Top {
  implicit def withConv[F[_], Before, After](
    implicit c: Cmp[F, Before],
    fn: After ⇒ Before,
  ):
    Cmp.Aux[F, After, c.Δ] = c.map
}

  trait FromHammerLab
extends WithConversion
{
  implicit def fromHammerLab[F[_]: Applicative, L, R](
    implicit
    ce: cmp.CanEq[L, R],
  ):
    Aux[F, L, R, ce.Diff] =
    new CanEq[F, L, R] {
      type Δ = ce.Diff
      def apply(l: L, r: R): Result = ce(l, r).pure[F]
    }
}

  trait FromLasersonLab
extends FromHammerLab
{
  implicit def fromLasersonLab[F[_]: Applicative, T](
    implicit
    ce: magnolia.Cmp[T],
  ):
    Cmp.Aux[F, T, ce.Δ] =
    new CanEq[F, T, T] {
      type Δ = ce.Δ
      def apply(l: T, r: T): Result = ce(l, r).pure[F]
    }
}

  trait FuturizeLeft
extends FromLasersonLab
{
  implicit def liftLeft[F[_]: Monad, L, R](
    implicit
    ce: CanEq[F, L, R],
  ):
    Aux[F, F[L], R, ce.Δ] =
    new CanEq[F, F[L], R] {
      type Δ = ce.Δ
      def apply(l: F[L], r: R): Result =
        for {
          l ← l
          res ← ce(l, r)
        } yield
          res
    }
}

 object CanEq
extends FuturizeLeft
{
  implicit def liftBoth[F[_]: Monad, L, R](
    implicit
    ce: CanEq[F, L, R],
  ):
    Aux[F, F[L], F[R], ce.Δ] =
    new CanEq[F, F[L], F[R]] {
      type Δ = ce.Δ
      def apply(l: F[L], r: F[R]): Result =
        for {
          l ← l
          r ← r
          res ← ce(l, r)
        } yield
          res
    }

  trait syntax {
    def cmp[F[_], L, R](l: L, r: R)(implicit c: CanEq[F, L, R]): F[?[c.Δ]] = c(l, r)
  }
}



