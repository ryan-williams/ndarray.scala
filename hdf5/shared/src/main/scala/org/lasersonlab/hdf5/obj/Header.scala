package org.lasersonlab.hdf5.obj

import java.time.Instant
import java.time.Instant.ofEpochSecond

import cats.implicits._
import hammerlab.option._
import org.lasersonlab.hdf5.Addr
import org.lasersonlab.hdf5.io.Buffer
import org.lasersonlab.hdf5.io.Buffer.{ MonadErr, UnsupportedValue, syntax }
import org.lasersonlab.hdf5.obj.Header.V2.{ MaxAttrs, Times }

object Header {
  case class V1(
    refCount: Int,
    msgs: Vector[Msg]
  )
  object V1 {
    def apply[F[+_]: MonadErr]()(implicit b: Buffer[F]): F[V1] = {
      val s = syntax(b); import s._
      for {
        _ ← expect("version", 1 toByte)
        _ ← zero("reserved")
        numMsgs ← unsignedShort()
        refCount ← signedInt("refCount")
        size ← unsignedInt()
        msgs ← b.takeBytes(size) {
          implicit b ⇒ Msg[F]
        }
      } yield
        V1(
          refCount,
          msgs
        )
    }
  }

  case class V2(
    times: ?[Times],
    maxAttrs: ?[MaxAttrs],
    msgs: Vector[Msg]
  )

  object V2 {
    def apply[F[+_]: MonadErr](implicit b: Buffer[F]): F[V2] = {
      val s = syntax(b); import s._
      for {
        _ ← expect("signature", Array[Byte]('O', 'H', 'D', 'R'))
        _ ← expect("version", 2 toByte)
        flags ← byte()
        times ←
          if ((flags & 0x10) > 0)
            for {
              accessed ← unsignedInt()
              modified ← unsignedInt()
              changed ← unsignedInt()
              created ← unsignedInt()
            } yield
              Some(
                Times(
                  ofEpochSecond(accessed),
                  ofEpochSecond(modified),
                  ofEpochSecond(changed),
                  ofEpochSecond(created)
                )
              )
          else
            None.pure[F]
        maxAttrs ←
          if ((flags & 0x4) > 0)
            for {
              compact ← unsignedShort()
              dense ← unsignedShort()
            } yield
              Some(
                MaxAttrs(
                  compact,
                  dense
                )
              )
          else
            None.pure[F]
        size ←
          flags & 0x3 match {
            case 0 ⇒ unsignedByte().map { _.toLong }
            case 1 ⇒ unsignedShort().map { _.toLong }
            case 2 ⇒ unsignedInt()
            case 3 ⇒ unsignedLong("size")
          }
        msgs ← b.takeBytes(size) {
          implicit b ⇒
            for {
              tpe ← unsignedByte()
              size ← unsignedShort()
              msgFlags ← byte()
              t ←
                if ((flags & 0x4) > 0)
                  for {
                    order ← unsignedShort()
                  } yield
                    (
                      Some(order),
                      2
                    )
                else
                  (None, 0).pure[F]
              (order, offset) = t
              msg ← Msg[F]
            } yield
              msg
        }
      } yield
        V2(
          times,
          maxAttrs,
          ???
        )
    }

    case class Times(
      accessed: Instant,
      modified: Instant,
      changed: Instant,
      created: Instant,
    )

    case class MaxAttrs(
      compact: Int,
      dense: Int
    )
  }

  sealed trait Msg
  object Msg {
    def apply[F[+_]: MonadErr](implicit b: Buffer[F]): F[Msg] = {
      val s = syntax(b); import s._
      for {
        pos ← b.position
        tpe ← unsignedShort()
        size ← unsignedShort()
        flags ← byte()
        _ ← expect("reserved", 0, 3)
        msg ← b.consume(size - 4) {
          implicit b ⇒
            tpe match {
              case  0 ⇒ NIL.pure[F]
              case  1 ⇒ Dataspace[F]
              case  2 ⇒ LinkInfo[F]
              case  3 ⇒ ??? // datatype
              case  4 ⇒ ??? // fill value (old)
              case  5 ⇒ ??? // fill value
              case  6 ⇒ ??? // link
              case  7 ⇒ ??? // external data files
              case  8 ⇒ ??? // data layout
              case  9 ⇒ ??? // bogus
              case 10 ⇒ ??? // group info
              case 11 ⇒ ??? // data storage – filter pipeline
              case 12 ⇒ ??? // attribute
              case 13 ⇒ Comment[F]
              case 14 ⇒ ??? // object modification time (old)
              case 15 ⇒ ??? // shared msg table
              case 16 ⇒ ??? // object header continuation
              case 17 ⇒ ??? // symbol table
              case 18 ⇒ ??? // object modification time
              case 19 ⇒ ??? // B-tree 'K' values
              case 20 ⇒ ??? // driver info
              case 21 ⇒ ??? // attr info
              case 22 ⇒ ??? // object ref count
              case 23 ⇒ ??? // file space info
              case n ⇒
                UnsupportedValue("header msg type", n, pos).raiseError[F, Msg]
            }
        }
      } yield
        msg
    }

    case object NIL extends Msg
    sealed trait Dataspace extends Msg
    object Dataspace {
      def apply[F[+_]: MonadErr](implicit b: Buffer[F]): F[Dataspace] = {
        val s = syntax(b); import s._
        for {
          version ← byte()
          dataspace ←
            version match {
              case 1 ⇒ V1[F]
              case 2 ⇒ V2[F]
              case n ⇒
                b.position.>>= {
                  pos ⇒
                    UnsupportedValue("dataspace version", n, pos)
                      .raiseError[F, Dataspace]
                }
            }
        } yield
          dataspace
      }

      case class Simple(dimensions: Vector[Dimension]) extends Dataspace
      case object Scalar extends Dataspace
      case object   Null extends Dataspace

      object V1 {
        def apply[F[+_]: MonadErr](implicit b: Buffer[F]): F[Simple] = {
          val s = syntax(b); import s._
          for {
            rank ← unsignedByte()
            flags ← byte()
            _ ← expect("reserved", 0, 5)
            dimensions ← Dimensions[F](rank, flags)
            _ ←
              if ((flags & 0x2) > 0)
                b.position.>>= {
                  pos ⇒
                    UnsupportedValue("flags – permutation idxs", 2, pos).raiseError[F, Unit]
                }
              else
                (()).pure[F]
          } yield
            Simple(dimensions)
        }
      }

      object V2 {
        def apply[F[+_]: MonadErr](implicit b: Buffer[F]): F[Dataspace] = {
          val s = syntax(b); import s._
          for {
            rank ← unsignedByte()
            flags ← byte()
            tpe ← byte()
            msg ←
              tpe match {
                case 0 ⇒ Scalar.pure[F]
                case 1 ⇒ Dimensions[F](rank, flags).map {Simple(_) }
                case 2 ⇒ Null.pure[F]
                case n ⇒
                  b.position.>>= {
                    pos ⇒
                      UnsupportedValue("msg type", n, pos)
                        .raiseError[F, Dataspace]
                  }
              }
          } yield
            msg
        }
      }

      case class Dimension(size: Long, max: ?[Long])

      object Dimensions {
        def apply[F[+_]: MonadErr](rank: Short, flags: Byte)(implicit b: Buffer[F]): F[Vector[Dimension]] = {
          val s = syntax(b); import s._
          for {
            sizes ← unsignedLongs("dimensions", rank)
            maxs ←
              if ((flags & 0x1) > 0)
                for {
                  maxs ← unsignedLongs("dimension maxs", rank)
                } yield
                  Some(maxs)
              else
                None.pure[F]
            dimensions ←
              maxs match {
                case None ⇒ sizes.map { Dimension(_, None) }.pure[F]
                case Some(maxs) if sizes.length == maxs.length ⇒
                  sizes
                    .zip(maxs)
                    .map {
                      case (size, max) ⇒ Dimension(size, Some(max))
                    }
                    .pure[F]
                case Some(maxs) ⇒
                  b.position.>>= {
                    pos ⇒
                      new IllegalStateException(
                        s"${sizes.length} dimension sizes but ${maxs.length} maxs found (ending at pos $pos)"
                      )
                      .raiseError[F, Vector[Dimension]]
                  }
              }
          } yield
            dimensions
        }
      }
    }

    case class LinkInfo(
      maxCreationIdx: ?[Long],
      fractalHeapAddr: ?[Addr],
      nameIndexBTree: ?[Addr],
      creationOrderIndex: ?[Addr]
    )
    extends Msg
    object LinkInfo {
      def apply[F[+_]: MonadErr](implicit b: Buffer[F]): F[LinkInfo] = {
        val s = syntax(b); import s._
        for {
          _ ← expect("version", 0 toByte)
          flags ← byte()
          maxCreationIdx ←
            if ((flags & 0x1) > 0)
              for {
                maxCreationIdx ← unsignedLong("maxCreationIdx")
              } yield
                Some(maxCreationIdx)
            else
              None.pure[F]
          fractalHeapAddr ← offset_?("fractalHeapAddr")
          nameIndexBTree ← offset_?("nameIndexBTree")
          creationOrderIndex ←
            if ((flags & 0x2) > 0)
              for {
                creationOrderIndex ← offset_?("creationOrderIndex")
              } yield
                creationOrderIndex
            else
              None.pure[F]
        } yield
          LinkInfo(
            maxCreationIdx,
            fractalHeapAddr,
            nameIndexBTree,
            creationOrderIndex
          )
      }
    }


    case class Comment(msg: String) extends Msg
    object Comment {
      def apply[F[+_]: MonadErr](implicit b: Buffer[F]): F[Comment] =
        b.ascii.map(Comment(_))
    }
  }
}
