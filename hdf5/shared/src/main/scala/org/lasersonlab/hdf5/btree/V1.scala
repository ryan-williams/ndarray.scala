package org.lasersonlab.hdf5.btree

import cats.implicits._
import hammerlab.either._
import hammerlab.collection._
import hammerlab.option._
import org.lasersonlab.hdf5.btree.V1.Node.{ Data, Group }
import org.lasersonlab.hdf5.{ Addr, Mask, UInt }
import org.lasersonlab.hdf5.io.Buffer
import org.lasersonlab.hdf5.io.Buffer.UnsupportedValue

object V1 {
  val MAGIC = Array[Byte]('T', 'R', 'E', 'E')
  def apply(K: Int)(implicit b: Buffer) = {
    import b._
    for {
      _ ← expect("magic", MAGIC)
      tpe ← Type()
      level = unsignedByte()
      numEntries = unsignedShort()
       left ← offset_?( "left sibling")
      right ← offset_?("right sibling")
      node ← {
        tpe match {
          case Type.`0` ⇒
            Group.children(K).map {
              Group(level, numEntries, left, right, _)
            }
          case Type.`1` ⇒
            Data.children(K).map {
              Data(level, numEntries, left, right, _)
            }
        }
      }
    } yield
      ()
  }

  sealed trait Type
  object Type {
    case object `0` extends Type
    case object `1` extends Type

    def apply()(implicit buf: Buffer) = {
      buf.buf.get() match {
        case 0 ⇒ R(`0`)
        case 1 ⇒ R(`1`)
        case n ⇒ L(UnsupportedValue("btree node type", n, buf.position() - 1))
      }
    }
  }

  sealed trait Node
  object Node {
    case class Group(
      level: Short,
      numEntries: Int,
       left: ?[Addr],
      right: ?[Addr],
      children: Seq[Group.Entry]
    )
    extends Node
    object Group {
      case class Entry(
        child: Addr,
        max: Key,
      )
      case class Key(address: Addr)

      def children(K: Int)(implicit b: Buffer): UnsupportedValue[Long] | Seq[Entry] = {
        import b._
        for {
          _ ← length("unused group key 0")
          children ←
            (
              for { i ← 0 until K toList } yield {
                for {
                  child ← offset(s"group child ptr $i")
                  len ← length(s"group child key $i")
                } yield
                  Entry(child, Key(len))
              }
            )
            .sequence
        } yield
          children
      }
    }

    case class Data(
      level: Short,
      numEntries: Int,
       left: ?[Addr],
      right: ?[Addr],
      children: Seq[Data.Entry]
    )
    extends Node
    object Data {
      case class Entry(
        min: Key,
        child: Addr,
      )
      case class Key(
        size: UInt,
        mask: Mask,
        idxs: Seq[Long]
      )
      def children(K: Int)(implicit b: Buffer): UnsupportedValue[_] | Seq[Entry] = {
        import b._
        for {
          children ←
            {
              for { i ← 0 until K toList } yield {
                val size = unsignedInt()
                val mask = Mask()
                for {
                  idxs ← unsignedLongs(s"data child $i idx")
                  key = Key(size, mask, idxs)
                  child ← offset(s"data child $i ptr")
                } yield
                  Entry(key, child)
              }
            }
            .sequence
          _ ← undefined(s"data child key $K")
        } yield
          children
      }
    }
  }

  sealed trait Key
}
