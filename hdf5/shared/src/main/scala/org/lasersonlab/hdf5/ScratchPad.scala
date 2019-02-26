package org.lasersonlab.hdf5

import org.lasersonlab.hdf5.io.Buffer

case class ScratchPad(
  btree: Long,
  nameHeap: Long
)
object ScratchPad {
  def apply()(implicit buffer: Buffer): Exception | ScratchPad = {
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

