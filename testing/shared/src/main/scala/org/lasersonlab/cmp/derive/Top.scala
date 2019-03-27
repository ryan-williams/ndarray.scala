package org.lasersonlab.cmp.derive

import hammerlab.option.?
import org.lasersonlab.cmp.CanEq

trait Top {
  type Aux[F[_], L, R, D] = CanEq[F, L, R] { type Δ = D }

  /** Make a [[CanEq]] from an `apply` function */
  def apply[F[_], L, R, D](f: (L, R) ⇒ F[?[D]]): Aux[F, L, R, D] =
    new CanEq[F, L, R] {
      type Δ = D
      def apply(l: L, r: R): Result = f(l, r)
    }
}
