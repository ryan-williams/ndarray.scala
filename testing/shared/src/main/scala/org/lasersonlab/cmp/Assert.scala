package org.lasersonlab.cmp

import cats.{ Functor, MonadError }
import cats.syntax.all._

import scala.reflect.ClassTag

trait Assert[F[_], L, R] {
  def apply(l: L, r: R): F[Unit]
}

object Assert
{
  case class ComparisonFailure[L, R, D](diff: D)(implicit l: ClassTag[L], r: ClassTag[R])
    extends RuntimeException(
      s"${l.runtimeClass.getSimpleName} vs ${r.runtimeClass.getSimpleName}: $diff"
    )

  def f[
    F[_]:  Functor,
    L   : ClassTag,
    R   : ClassTag,
  ](
    l: L,
    r: R
  )(
    implicit
    c: CanEq[F, L, R]
  ):
    F[Unit] =
    c(l, r)
      .map {
        case Some(diff) ⇒ throw ComparisonFailure[L, R, Any](diff)
        case None       ⇒ ()
      }

  implicit def liftNeither[
    F[_]:  Functor,
    L   : ClassTag,
    R   : ClassTag,
  ](
    implicit
    c: CanEq[F, L, R]
  ):
    Assert[F, L, R]
  =
    f(_, _)

  type MonadErr[F[_]] = MonadError[F, Throwable]

  trait syntax {
    def ==[F[_], L: ClassTag, R: ClassTag](l: L, r: R)(implicit a: Assert[F, L, R]): F[Unit] = a(l, r)
    def !![F[_]: MonadErr, Δ](Δ: Δ): F[Unit] = ComparisonFailure(Δ).raiseError[F, Unit]
  }
}
