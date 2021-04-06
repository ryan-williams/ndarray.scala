package org.lasersonlab.cmp

import cats.syntax.all._
import hammerlab.option._
import org.lasersonlab.cmp.CanEq.Aux
import org.lasersonlab.cmp.derive.LiftLeft

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

object CanEq
  extends LiftLeft
{
  trait syntax {
    def cmp[F[_], L, R](l: L, r: R)(implicit c: CanEq[F, L, R]): F[?[c.Δ]] = c(l, r)
  }
}



