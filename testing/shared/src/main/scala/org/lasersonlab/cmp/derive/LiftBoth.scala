package org.lasersonlab.cmp.derive

import cats.Monad
import cats.syntax.all._
import org.lasersonlab.cmp.CanEq

trait LiftBoth
extends LiftLeft
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
}
