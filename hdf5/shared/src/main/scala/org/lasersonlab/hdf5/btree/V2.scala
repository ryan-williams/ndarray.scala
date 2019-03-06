package org.lasersonlab.hdf5.btree

import cats.implicits._
import org.lasersonlab.hdf5.{ Addr, Length }
import org.lasersonlab.hdf5.io.Buffer
import org.lasersonlab.hdf5.io.Buffer.{ MonadErr, UnsupportedValue, syntax }

object V2 {

  case class Header(
    tpe: Type,
    recordSize: Int,
    depth: Int,
    splitPercent: Byte,
    mergePercent: Byte,
    rootAddr: Addr,
    numRootRecords: Int,
    totalRecords: Length,
  )
  object Header {
    def apply[F[+_]: MonadErr](implicit b: Buffer[F]): F[Header] = {
      val s = syntax(b); import s._
      for {
        pos ← b.position
        _ ← expect("signature", Array[Byte]('B', 'T', 'H', 'D'))
        _ ← zero("version")
        tpe ← byte().>>= { Type[F](_, pos) }
        size ← unsignedInt()
        recordSize ← unsignedShort()
        depth ← unsignedShort()
        splitPercent ← byte()
        mergePercent ← byte()
        rootAddr ← offset("root addr")
        numRootRecords ← unsignedShort()
        totalRecords ← length("total records")
        checksum ← bytes("checksum", 4)
      } yield
        Header(
          tpe,
          recordSize,
          depth,
          splitPercent,
          mergePercent,
          rootAddr,
          numRootRecords,
          totalRecords
        )
    }
  }

  sealed trait Type
  object Type {
    def apply[F[+_]: MonadErr](byte: Byte, pos: Long): F[Type] =
      byte match {
        case 1 ⇒ Objects(direct = false, filtered = false).pure[F]
        case 2 ⇒ Objects(direct = false, filtered =  true).pure[F]
        case 3 ⇒ Objects(direct =  true, filtered = false).pure[F]
        case 4 ⇒ Objects(direct =  true, filtered =  true).pure[F]
        case 5 ⇒ Index(name =  true, attrs = false)       .pure[F]
        case 6 ⇒ Index(name = false, attrs = false)       .pure[F]
        case 7 ⇒ SharedHeader                             .pure[F]
        case 8 ⇒ Index(name =  true, attrs =  true)       .pure[F]
        case 9 ⇒ Index(name = false, attrs =  true)       .pure[F]
        case 10 ⇒ Chunks(filtered = false)                .pure[F]
        case 11 ⇒ Chunks(filtered = false)                .pure[F]
        case n ⇒
          UnsupportedValue("v2 btree type", n, pos).raiseError[F, Type]
      }

    case class Objects(direct: Boolean, filtered: Boolean) extends Type
    case class Index(name: Boolean, attrs: Boolean) extends Type
    case class Chunks(filtered: Boolean) extends Type
    case object SharedHeader extends Type
  }

}
