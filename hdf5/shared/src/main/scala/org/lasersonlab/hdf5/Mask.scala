package org.lasersonlab.hdf5

import cats.implicits._
import org.lasersonlab.hdf5.io.Buffer
import org.lasersonlab.hdf5.io.Buffer.MonadErr

import scala.collection.immutable.BitSet
import BitSet.fromBitMask

case class Mask(bits: BitSet)
object Mask {
  def apply[F[+_]: MonadErr](implicit b: Buffer[F]): F[Mask] = {
    import b._
    for {
      l1 ← b.getLong
      l2 ← b.getLong
    } yield
      Mask(
        fromBitMask(
          Array(
            l1,
            l2
          )
        )
      )
  }
}
