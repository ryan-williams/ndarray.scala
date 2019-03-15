package org.lasersonlab.hdf5.btree

import cats.implicits._
import hammerlab.option._
import org.lasersonlab.hdf5.btree.V2.Node.Internal.Child
import org.lasersonlab.hdf5.btree.V2.Node.{ Internal, Leaf }
import org.lasersonlab.hdf5.heap.Fractal
import org.lasersonlab.hdf5.{ Addr, Length, Mask }
import org.lasersonlab.hdf5.io.Buffer
import org.lasersonlab.hdf5.io.Buffer.{ MonadErr, UnsupportedValue }

import Array.fill

object V2 {

  def byteSize(n: Long, check: Int = 1): Int =
    if (check == 8) 8
    else if ((n >> (check * 8)) > 0) byteSize(n, check + 1)
    else check

  case class MaxChildRecords(children: Int, descendents: Long, tail: ?[MaxChildRecords])
  object MaxChildRecords {
    def apply(depth: Int, nodeSize: Int, recordSize: Int): MaxChildRecords =
      depth match {
        case 1 ⇒ MaxChildRecords((nodeSize - 10) / recordSize, (nodeSize - 10) / recordSize, None)
        case 2 ⇒
          val leafMaxs @ MaxChildRecords(maxLeafRecords, _, None) = apply(depth - 1, nodeSize, recordSize)
          val maxChildren = (nodeSize - 10 - recordSize) / (recordSize + 8 + byteSize(maxLeafRecords))
          val maxDescendents = maxChildren.toLong * maxLeafRecords
          MaxChildRecords(maxChildren, maxDescendents, Some(leafMaxs))
        case _ ⇒
          val childMaxs @ MaxChildRecords(maxChildRecords, maxChildDescendents, _) = apply(depth - 1, nodeSize, recordSize)
          val maxChildren = (nodeSize - 10 - recordSize) / (recordSize + 8 + byteSize(maxChildRecords) + byteSize(maxChildDescendents))
          val maxDescendents = maxChildRecords.toLong * maxChildDescendents
          MaxChildRecords(maxChildRecords, maxDescendents, Some(childMaxs))
      }
  }

  case class Header(
    tpe: Type,
    nodeSize: Int,
    recordSize: Int,
    depth: Int,
    splitPercent: Byte,
    mergePercent: Byte,
    rootAddr: Addr,
    numRootRecords: Int,
    totalRecords: Length,
  ) {
    def root[F[+_]: MonadErr](implicit b: Buffer[F]): F[Node] =
      depth match {
        case 0 ⇒ Leaf[F](tpe, numRootRecords)
        case _ ⇒ Internal[F](tpe, depth, numRootRecords, MaxChildRecords(depth, nodeSize, recordSize))
      }
  }
  object Header {
    def apply[F[+_]: MonadErr](implicit b: Buffer[F]): F[Header] = {
      import b._
      for {
        pos ← b.position
        _ ← expect("signature", Array[Byte]('B', 'T', 'H', 'D'))
        _ ← zero("version")
        typeByte ← byte()
        tpe ← Type[F](typeByte, pos)
        nodeSize ← signedInt("node size")  //
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
          nodeSize,
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

  sealed abstract class Type(val byte: Byte) {
    type Record <: Type.Record
    def apply[F[+_]: MonadErr](implicit b: Buffer[F]): F[Record]
  }

  object Type {
    type Aux[R <: Record] = Type { type Record = R }
    def apply[F[+_]: MonadErr](byte: Byte, pos: Long): F[Type] =
      byte match {
        case  1 ⇒ IndirectUnfiltered  .pure[F]
        case  2 ⇒   IndirectFiltered  .pure[F]
        case  3 ⇒   DirectUnfiltered  .pure[F]
        case  4 ⇒     DirectFiltered  .pure[F]
        case  5 ⇒ GroupNames          .pure[F]
        case  6 ⇒ GroupCreationOrders .pure[F]
        case  7 ⇒ SharedHeader        .pure[F]
        case  8 ⇒  AttrNames          .pure[F]
        case  9 ⇒  AttrCreationOrders .pure[F]
        case 10 ⇒ UnfilteredChunks    .pure[F]
        case 11 ⇒   FilteredChunks    .pure[F]
        case n ⇒
          UnsupportedValue("v2 btree type", n, pos).raiseError[F, Type]
      }

    sealed trait Record

    sealed abstract class Objects(byte: Byte, direct: Boolean, filtered: Boolean) extends Type(byte)
    case object IndirectUnfiltered extends Objects(1, false, false) {
      case class Record(addr: Addr, length: Length, heapID: Length) extends Type.Record
      def apply[F[+_]: MonadErr](implicit b: Buffer[F]): F[Record] = {
        import b._
        for {
          addr ← offset("addr")
          length ← b.length("length")
          heapId ← b.length("heap ID")
        } yield
          Record(addr, length, heapId)
      }
    }
    case object   IndirectFiltered extends Objects(2, false,  true) {
      case class Record(addr: Addr, length: Length, filtered: Filtered, heapID: Length) extends Type.Record
      def apply[F[+_]: MonadErr](implicit b: Buffer[F]): F[Record] = {
        import b._
        for {
          addr ← offset("addr")
          length ← b.length("length")
          filtered ← Filtered[F]
          heapId ← b.length("heap ID")
        } yield
          Record(addr, length, filtered, heapId)
      }
    }
    case object   DirectUnfiltered extends Objects(3,  true, false) {
      case class Record(addr: Addr, length: Length                                    ) extends Type.Record
      def apply[F[+_]: MonadErr](implicit b: Buffer[F]): F[Record] = {
        import b._
        for {
          addr ← offset("addr")
          length ← length("length")
        } yield
          Record(addr, length)
      }
    }
    case object     DirectFiltered extends Objects(4,  true,  true) {
      case class Record(addr: Addr, length: Length, filtered: Filtered                ) extends Type.Record
      def apply[F[+_]: MonadErr](implicit b: Buffer[F]): F[Record] = {
        import b._
        for {
          addr ← offset("addr")
          length ← length("length")
          filtered ← Filtered[F]
        } yield
          Record(addr, length, filtered)
      }
    }

    case object SharedHeader extends Type(7) {
      case class Record(
        header: Boolean,
        hash: Int,
        refCount: Int,
        heapId: Long
      )
      extends Type.Record
      def apply[F[+_]: MonadErr](implicit b: Buffer[F]): F[Record] = {
        import b._
        for {
          header ← bool("msg in object header")
          hash ← signedInt("hash")
          refCount ← signedInt("ref count")
          heapId ← b.getLong
        } yield
          Record(header, hash, refCount, heapId)
      }
    }

    sealed abstract class Index(byte: Byte, name: Boolean, attrs: Boolean) extends Type(byte)
    case object GroupNames          extends Index(5,  true, false) {
      case class Record(hash: Int, id: Fractal.Id) extends Type.Record
      def apply[F[+_]: MonadErr](implicit b: Buffer[F]): F[Record] = {
        import b._
        for {
          hash ← signedInt("hash")
          bytes ← bytes("fractal heap id", 7)
        } yield
          Record(hash, Fractal.Id(bytes))
      }
    }
    case object GroupCreationOrders extends Index(6, false, false) {
      case class Record(creationOrder: Long, id: Fractal.Id) extends Type.Record
      def apply[F[+_]: MonadErr](implicit b: Buffer[F]): F[Record] = {
        import b._
        for {
          creationOrder ← b.getLong
          bytes ← bytes("fractal heap id", 7)
        } yield
          Record(creationOrder, Fractal.Id(bytes))
      }
    }
    case object  AttrNames          extends Index(8,  true,  true) {
      case class Record(id: Fractal.Id, flags: Byte, creationOrder: Int, nameHash: Int) extends Type.Record
      def apply[F[+_]: MonadErr](implicit b: Buffer[F]): F[Record] = {
        import b._
        for {
          id ← bytes("fractal heap id", 8)
          flags ← b.get
          creationOrder ← b.getInt
          hash ← b.getInt
        } yield
          Record(Fractal.Id(id), flags, creationOrder, hash)
      }
    }
    case object  AttrCreationOrders extends Index(9, false,  true) {
      case class Record(id: Fractal.Id, flags: Byte, creationOrder: Int) extends Type.Record
      def apply[F[+_]: MonadErr](implicit b: Buffer[F]): F[Record] = {
        import b._
        for {
          id ← bytes("fractal heap id", 8)
          flags ← b.get
          creationOrder ← b.getInt
          hash ← b.getInt
        } yield
          Record(Fractal.Id(id), flags, creationOrder)
      }
    }

    case class Filtered(
      mask: Mask,
      memSize: Length
    )
    object Filtered {
      def apply[F[+_]: MonadErr](implicit b: Buffer[F]): F[Filtered] = {
        import b._
        for {
          mask ← Mask[F]
          memSize ← length("filtered object memory size")
        } yield
          Filtered(
            mask,
            memSize
          )
      }
    }

    sealed abstract class Chunks(byte: Byte, filtered: Boolean) extends Type(byte)
    case object UnfilteredChunks extends Chunks(10, false) {
      case class Record(addr: Addr, offsets: Vector[Long]) extends Type.Record
      def apply[F[+_]: MonadErr](implicit b: Buffer[F]): F[Record] = {
        import b._
        for {
          addr ← offset("addr")
          offsets = ???  // need to know number of dimensions…?
        } yield
          Record(addr, offsets)
      }
    }
    case object FilteredChunks extends Chunks(11, true) {
      case class Record(addr: Addr, size: Long, mask: Mask, offsets: Vector[Long]) extends Type.Record
      def apply[F[+_]: MonadErr](implicit b: Buffer[F]): F[Record] = {
        import b._
        for {
          addr ← offset("addr")
          size = ???  // variable size; need from context
          mask ← Mask[F]
          offsets = ???  // need to know number of dimensions…?
        } yield
          Record(addr, size, mask, offsets)
      }
    }

    object Objects {
      def apply[F[+_]: MonadErr](tpe: Byte)(implicit b: Buffer[F]): F[Unit] = {
        import b._
        for {
          pos ← b.position
          addr ← offset("object address")
          length ← length("object length")
        } yield
          ???
      }
    }
  }

  sealed trait Node
  object Node {

    case class Internal(
      children: Vector[Child]
    )
    extends Node

    object Internal {
      case class Child(
        addr: Addr,
        records: Long,
        totalRecords: ?[Long]
      )
      object Child {
        def apply[F[+_]: MonadErr](depth: Int, numRecordsSize: Int, totalRecordsSize: Int)(implicit b: Buffer[F]): F[Child] = {
          import b._
          for {
            addr ← offset("addr")
            numRecords ← b.getN(numRecordsSize, fill(8)(0 toByte), _.getLong)
            totalRecords ←
              if (depth > 1)
                b.getN(totalRecordsSize, fill(8)(0 toByte), _.getLong).map(Some(_))
              else
                None.pure[F]
          } yield
            Child(
              addr,
              numRecords,
              totalRecords
            )
        }
      }

      def apply[F[+_]: MonadErr](
        tpe: Type,
        depth: Int,
        numRecords: Int,
        maxs: MaxChildRecords
      )(
        implicit
        b: Buffer[F]
      ):
        F[Internal] = {
        import b._
        for {
          _ ← expect("signature", Array[Byte]('B', 'T', 'I', 'N'))
          _ ← zero("version")
          _ ← expect("type", tpe.byte)
          records ← b.takeN(numRecords + 1) { implicit b ⇒ tpe[F] }
          children ← b.takeN(numRecords) { implicit b ⇒ Child[F](depth, byteSize(maxs.children), byteSize(maxs.descendents)) }
          checksum = ???
        } yield
          Internal(children)
      }
    }

    case class Leaf[R <: Type.Record](tpe: Type.Aux[R], records: Vector[R]) extends Node
    object Leaf {
      def apply[F[+_]: MonadErr](tpe: Type, N: Int)(implicit b: Buffer[F]): F[Leaf[tpe.Record]] = {
        import b._
        for {
          _ ← expect("signature", Array[Byte]('B', 'T', 'L', 'F'))
          _ ← zero("version")
          _ ← expect("type", tpe.byte)
          records ← b.takeN(N) { implicit b ⇒ tpe[F] }
          checksum = ???
        } yield
          Leaf(tpe, records)
      }
    }
  }

}
