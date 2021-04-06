package org.lasersonlab

package object cmp {
  type Cmp[F[_], T] = CanEq[F, T, T]
  object Cmp {
    type Aux[F[_], T, D] = CanEq[F, T, T] { type Δ = D }
  }
}
