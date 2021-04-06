package org.lasersonlab.cmp

import cats.{ Functor, MonadError }
import cats.syntax.all._

import scala.reflect.ClassTag

trait Assert[F[_], L, R] {
  def apply(l: L, r: R): F[Unit]
}

object Assert
{
  implicit def   objectToClass[T](t: T)(implicit ct: ClassTag[T]): Class[_] = ct.runtimeClass
  implicit def classTagtoClass[T](ct: ClassTag[T]): Class[_] = ct.runtimeClass

  case class ComparisonFailure[F[_], L, R, D](diff: D)(implicit ce: CanEq[F, L, R], l: ClassTag[L], r: ClassTag[R])
    extends RuntimeException(
      s"$ce: ${l.getSimpleName} vs ${r.getSimpleName}: $diff"
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
        case Some(diff) ⇒ throw ComparisonFailure[F, L, R, Any](diff)
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
  }
}
