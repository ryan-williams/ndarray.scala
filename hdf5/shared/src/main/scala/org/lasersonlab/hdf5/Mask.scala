package org.lasersonlab.hdf5

import org.lasersonlab.hdf5.io.Buffer

import scala.collection.immutable.BitSet
import BitSet.fromBitMask

case class Mask(bits: BitSet)
object Mask {
  def apply()(implicit b: Buffer): Mask = {
    import b._
    Mask(
      fromBitMask(
        Array(
          b.getLong,
          b.getLong)
      )
    )
  }
}
