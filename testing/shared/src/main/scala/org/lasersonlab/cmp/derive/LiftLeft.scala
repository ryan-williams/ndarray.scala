package org.lasersonlab.cmp.derive

import cats.Monad
import cats.syntax.all._
import org.lasersonlab.cmp.CanEq

  trait LiftLeft
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
