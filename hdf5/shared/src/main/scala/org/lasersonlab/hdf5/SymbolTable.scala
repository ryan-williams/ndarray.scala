package org.lasersonlab.hdf5

import hammerlab.either._
import hammerlab.option._
import org.lasersonlab.hdf5.io.Buffer
import org.lasersonlab.hdf5.io.Buffer.UnsupportedValue

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

  case class Node(
    entries: Vector[Entry]
  )

  sealed trait Entry
  object Entry {

    def apply()(implicit b: Buffer): UnsupportedValue[_] | Entry = {
      import b._
      val pos = b.position()
      for {
        nameOffset ← offset  (   "name offset")
              addr ← offset_?("header address")
        cacheType = unsignedByte()
        _ ← expect("reserved", 0, 4)
        entry ← cacheType match {
          case 0 ⇒
            for {
              addr ← addr.fold[UnsupportedValue[Long] | Addr](L(UnsupportedValue("name offset", -1L, pos + 4)))(R(_))
            } yield {
              // burn 16 bytes of unused "scratch" space
              b.getLong; b.getLong
              Object(nameOffset, addr, None)
            }
          case 1 ⇒
            for {
              addr ← addr.fold[UnsupportedValue[Long] | Addr](L(UnsupportedValue("name offset", -1L, pos + 4)))(R(_))
              scratchPad ← ScratchPad()
            } yield
              Object(nameOffset, addr, Some(scratchPad))
          case 2 ⇒
            val localHeapOffset = unsignedInt()
            // burn the remaining 12 bytes of scratchpad
            buf.getInt; buf.getLong
            R(Link(nameOffset, localHeapOffset))
          case n ⇒
            L(
              UnsupportedValue("cache type", n, b.position() - 1)
            )
        }
      } yield
        entry
    }

    case class Object(
      nameOffset: Addr,
      addr: Addr,
      scratchPad: ?[ScratchPad]
    )
    extends Entry

    case class Link(
      nameOffset: Addr,
      localHeapOffset: Long
    )
    extends Entry
  }
}

