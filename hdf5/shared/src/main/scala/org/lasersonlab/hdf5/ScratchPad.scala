package org.lasersonlab.hdf5

import hammerlab.either._
import org.lasersonlab.hdf5.io.Buffer
import org.lasersonlab.hdf5.io.Buffer.UnsupportedValue

case class ScratchPad(
  btree: Long,
  nameHeap: Long
)
object ScratchPad {
  def apply()(implicit buffer: Buffer): UnsupportedValue[Long] | ScratchPad = {
    import buffer._
    for {
      btree ← offset("btree")
      nameHeap ← offset("nameHeap")
    } yield
      ScratchPad(
        btree,
        nameHeap
      )
  }
}

