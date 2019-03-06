package org.lasersonlab.hdf5.btree

import cats.implicits._
import hammerlab.collection._
import hammerlab.option._
import org.lasersonlab.hdf5.btree.V1.Node.{ Data, Group }
import org.lasersonlab.hdf5.{ Addr, Mask, UInt }
import org.lasersonlab.hdf5.io.Buffer
import org.lasersonlab.hdf5.io.Buffer.{ MonadErr, UnsupportedValue, syntax }

object V1 {
  val MAGIC = Array[Byte]('T', 'R', 'E', 'E')
  def apply[F[+_]: MonadErr](K: Int)(implicit b: Buffer[F]): F[Unit] = {
    val s = syntax(b); import s._
    for {
      _ ← expect("magic", MAGIC)
      tpe ← Type[F]()
      level ← unsignedByte()
      numEntries ← unsignedShort()
       left ← offset_?( "left sibling")
      right ← offset_?("right sibling")
      node ← {
        tpe match {
          case Type.`0` ⇒
            Group.children[F](K).map {
              Group(level, numEntries, left, right, _)
            }
          case Type.`1` ⇒
            Data.children[F](K).map {
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

    def apply[F[+_]: MonadErr]()(implicit b: Buffer[F]): F[Type] = {
      for {
        pos ← b.position
        byte ← b.get
        res ←
          byte match {
            case 0 ⇒ `0`.pure[F]
            case 1 ⇒ `1`.pure[F]
            case n ⇒ UnsupportedValue("btree node type", n, pos).raiseError[F, Type]
          }
      } yield
        res
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

      def children[F[+_]: MonadErr](K: Int)(implicit b: Buffer[F]): F[Seq[Entry]] = {
        val s = syntax(b); import s._
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
      def children[F[+_]: MonadErr](K: Int)(implicit b: Buffer[F]): F[Seq[Entry]] = {
        val s = syntax(b); import s._
        for {
          children ←
            {
              for { i ← 0 until K toList } yield {
                for {
                  size ← unsignedInt()
                  mask ← Mask[F]
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
