package org.lasersonlab.hdf5

import cats.implicits._
import org.lasersonlab.hdf5.io.Buffer
import org.lasersonlab.hdf5.io.Buffer.MonadErr

case class ScratchPad(
  btree: Long,
  nameHeap: Long
)
object ScratchPad {
  def apply[F[+_]: MonadErr]()(implicit b: Buffer[F]): F[ScratchPad] = {
    import b._
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
