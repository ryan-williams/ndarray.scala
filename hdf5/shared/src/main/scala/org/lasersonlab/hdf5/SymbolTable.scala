package org.lasersonlab.hdf5

import cats.implicits._
import hammerlab.either._
import hammerlab.option._
import org.lasersonlab.hdf5.io.Buffer
import org.lasersonlab.hdf5.io.Buffer.{ MonadErr, UnsupportedValue }

//case class SymbolTable(
//  name: Long,
//  header: Long,
//  scratch: ScratchPad
//)
object SymbolTable {
//  def apply[F[+_]: MonadErr]()(implicit b: Buffer[F]): F[SymbolTable] = {
//    import b._
//    for {
//      name ← offset("name")
//      header ← offset("header")
//      _ ← int("cache type", 1)
//      _ ← expect("reserved", 0, 4)
//      scratch ← ScratchPad[F]()
//    } yield
//      SymbolTable(
//        name,
//        header,
//        scratch
//      )
//  }

  case class Node(
    entries: Vector[Entry]
  )

  sealed trait Entry
  object Entry {

    def apply[F[+_]: MonadErr]()(implicit b: Buffer[F]): F[Entry] = {
      import b._
      for {
        pos ← position
        nameOffset ← offset  (   "name offset")
              addr ← offset_?("header address")
        cacheType ← unsignedByte()
        _ ← expect("reserved", 0, 7)
        entry ← cacheType match {
          case 0 ⇒
            for {
              addr ← addr.fold[F[Addr]](UnsupportedValue("name offset", -1L, pos + 4).raiseError[F, Addr])(_.pure[F])
              // burn 16 bytes of unused "scratch" space
              _ ← burn(16)
            } yield {
              Object(nameOffset, addr, None)
            }
          case 1 ⇒
            for {
              addr ← addr.fold[F[Addr]](UnsupportedValue("name offset", -1L, pos + 4).raiseError[F, Addr])(_.pure[F])
              scratchPad ← ScratchPad[F]()
            } yield
              Object(nameOffset, addr, Some(scratchPad))
          case 2 ⇒
            for {
              localHeapOffset ← unsignedInt()
              // burn the remaining 12 bytes of scratchpad
              _ ← burn(12)
            } yield
              Link(
                nameOffset,
                localHeapOffset
              )
          case n ⇒
            UnsupportedValue("cache type", n, pos).raiseError[F, Link]
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

