package org.lasersonlab.hdf5

import cats.implicits._
import hammerlab.either._
import hammerlab.option._
import org.lasersonlab.hdf5.io.Buffer
import org.lasersonlab.hdf5.io.Buffer.{ MonadErr, UnsupportedValue, syntax }

case class SymbolTable(
  name: Long,
  header: Long,
  scratch: ScratchPad
)
object SymbolTable {
  def apply[F[+_]: MonadErr]()(implicit b: Buffer[F]): F[SymbolTable] = {
    val s = syntax(b); import s._
    for {
      name ← offset("name")
      header ← offset("header")
      _ ← int("cache type", 1)
      _ ← expect("reserved", 0, 4)
      scratch ← ScratchPad[F]()
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

    def apply[F[+_]: MonadErr]()(implicit b: Buffer[F]): F[Entry] = {
      val s = syntax(b); import s._
      for {
        pos ← b.position
        nameOffset ← offset  (   "name offset")
              addr ← offset_?("header address")
        cacheType ← unsignedByte()
        _ ← expect("reserved", 0, 4)
        entry ← cacheType match {
          case 0 ⇒
            for {
              addr ← addr.fold[F[Addr]](UnsupportedValue("name offset", -1L, pos + 4).raiseError[F, Addr])(_.pure[F])
            } yield {
              // burn 16 bytes of unused "scratch" space
              b.getLong; b.getLong
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
              _ ← buf.burn(12)
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

