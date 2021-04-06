package org.lasersonlab.cmp.derive

import org.lasersonlab.cmp.Cmp

  trait WithConversion
extends Top {
  implicit def withConv[
    F[_],
    Before,
    After
  ](
    implicit
    c: Cmp[F, Before],
    fn: After ⇒ Before,
  ):
    Cmp.Aux[F, After, c.Δ]
  =
    c.map
}
