package org.lasersonlab.test.future

import hammerlab.option._
import lasersonlab.future._
import org.hammerlab.cmp
import org.lasersonlab.test.future.CanEq.Aux

import scala.concurrent.{ ExecutionContext, Future }
import scala.reflect.ClassTag

trait CanEq[L, R] {
  type Δ
  type Result = F[Option[Δ]]
  def apply(l: L, r: R): F[?[Δ]]

  def map[L1, R1](
    implicit
    fl: L1 ⇒ L,
    fr: R1 ⇒ R
  ):
      Aux[L1, R1, Δ] =
    CanEq[L1, R1, Δ] {
      (l, r) ⇒ this(fl(l), fr(r))
    }
  def map[T](
    f: T ⇒ L
  )(
    implicit
    ev: L =:= R
  ):
      Aux[T, T, Δ] =
    CanEq[T, T, Δ] {
      (l, r) ⇒ this(f(l), f(r))
    }
}

trait Top {
  type Aux[L, R, D] = CanEq[L, R] { type Δ = D }

  def apply[L, R, D](f: (L, R) ⇒ F[?[D]]): Aux[L, R, D] =
    new CanEq[L, R] {
      type Δ = D
      def apply(l: L, r: R): Result = f(l, r)
    }
}

  trait WithConversion
extends Top {
  implicit def withConv[Before, After](
    implicit c: Cmp[Before],
    fn: After ⇒ Before,
  ):
    Cmp.Aux[After, c.Δ] = c.map
}

  trait FromHammerLab
extends WithConversion
{
  implicit def fromHammerLab[L, R](
    implicit
    ce: cmp.CanEq[L, R],
    ec: ExecutionContext
  ):
    Aux[L, R, ce.Diff] =
    new CanEq[L, R] {
      type Δ = ce.Diff
      def apply(l: L, r: R): Result = F { ce(l, r) }
    }
}

  trait FromLasersonLab
extends FromHammerLab
{
  implicit def fromLasersonLab[T](
    implicit
    ce: org.lasersonlab.test.Cmp[T],
    ec: ExecutionContext
  ):
    Cmp.Aux[T, ce.Diff] =
    new CanEq[T, T] {
      type Δ = ce.Diff
      def apply(l: T, r: T): Result = F { ce(l, r) }
    }
}

  trait FuturizeLeft
extends FromLasersonLab
{
  implicit def futurizeLeft[L, R](
    implicit
    ce: CanEq[L, R],
    ec: ExecutionContext
  ):
    Aux[F[L], R, ce.Δ] =
    new CanEq[F[L], R] {
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
  implicit def futurizeBoth[L, R](
    implicit
    ce: CanEq[L, R],
    ec: ExecutionContext
  ):
    Aux[F[L], F[R], ce.Δ] =
    new CanEq[F[L], F[R]] {
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
    def cmp[L, R](l: L, r: R)(implicit c: CanEq[L, R]): Future[Option[c.Δ]] = c(l, r)
  }
}

trait Assert[L, R] {
  def apply(l: L, r: R): F[Unit]
}
object Assert
{
  case class ComparisonFailure[L, R, D](diff: D)(implicit l: ClassTag[L], r: ClassTag[R])
    extends RuntimeException(
      s"${l.runtimeClass.getSimpleName} vs ${r.runtimeClass.getSimpleName}: $diff"
    )

  def f[L: ClassTag, R: ClassTag](l: L, r: R)(implicit c: CanEq[L, R], ec: ExecutionContext): F[Unit] =
    c(l, r).map {
      case Some(diff) ⇒ throw ComparisonFailure[L, R, Any](diff)
      case None       ⇒ ()
    }

  implicit def futureizeNeither[L: ClassTag, R: ClassTag](implicit c: CanEq[L, R], ec: ExecutionContext): Assert[L, R] = f(_, _)

  trait syntax {
    def ==[L: ClassTag, R: ClassTag](l: L, r: R)(implicit a: Assert[L, R]): F[Unit] = a(l, r)
    def !![Diff](diff: Diff): F[Unit] = Future.failed(ComparisonFailure(diff))
  }
}
