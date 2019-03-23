package org.lasersonlab.cmp.derive

import cats.Applicative
import cats.syntax.all._
import org.hammerlab.cmp
import org.lasersonlab.cmp.CanEq

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
