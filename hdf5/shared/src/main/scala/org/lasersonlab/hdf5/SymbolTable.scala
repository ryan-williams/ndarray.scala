package org.lasersonlab.hdf5

import org.lasersonlab.hdf5.io.Buffer

case class SymbolTable(
  name: Long,
  header: Long,
  scratch: ScratchPad
)
object SymbolTable {
  def apply()(implicit buffer: Buffer): Exception | SymbolTable = {
    import buffer._
    for {
      name ← offset("name")
      header ← offset("header")
      _ ← int("cache type", 1)
      _ ← expect("reserved", 0, 4)
      scratch ← ScratchPad()
    } yield
      SymbolTable(
        name,
        header,
        scratch
      )
  }
}

