package org.lasersonlab.zarr

import org.lasersonlab.zarr.io.Load
import org.lasersonlab.zarr.utils.Idx
import org.scalatest.FunSuite

trait HasGetOps {
  self: FunSuite ⇒
  /**
   * Syntax for unwrapping an [[Either]], failing a test case with any [[Throwable]], if present
   */
  implicit class GetOps[T](val t: Either[Throwable, T]) {
    def ! : T = t.fold(fail(_), identity)
  }
}

abstract class Suite
  extends hammerlab.Suite
     with HasGetOps
     with Load.syntax {
  implicit val __int: Idx.T[Int] = Idx.Int
}
