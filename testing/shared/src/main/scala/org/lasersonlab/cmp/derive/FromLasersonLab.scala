package org.lasersonlab.cmp.derive

import cats.Applicative
import cats.syntax.all._
import org.lasersonlab.cmp.{ CanEq, Cmp, magnolia }

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
