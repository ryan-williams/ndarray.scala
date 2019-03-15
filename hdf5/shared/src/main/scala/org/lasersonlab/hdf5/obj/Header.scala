package org.lasersonlab.hdf5.obj

import java.nio.{ ByteBuffer, ByteOrder }
import java.time.Instant
import java.time.Instant.ofEpochSecond
import java.nio.ByteOrder.{ BIG_ENDIAN, LITTLE_ENDIAN }
import java.nio.charset.StandardCharsets.{ US_ASCII, UTF_8 }

import cats.implicits.{ catsStdInstancesForEither ⇒ _, catsStdInstancesForTry ⇒ _, _ }
import hammerlab.option._
import org.lasersonlab.hdf5.{ Addr, Length }
import org.lasersonlab.hdf5.io.Buffer
import org.lasersonlab.hdf5.io.Buffer.{ MonadErr, UnsupportedValue }
import org.lasersonlab.hdf5.obj.Header.Msg.Datatype.FloatingPoint.MantissaNormalization
import org.lasersonlab.hdf5.obj.Header.Msg.FillValue.{ AllocationTime, WriteTime }
import org.lasersonlab.hdf5.obj.Header.V2.{ MaxAttrs, Times }
import org.lasersonlab.math.utils.roundUp

import scala.collection.immutable.BitSet
import BitSet.fromBitMask

sealed trait Header
object Header {

  type Str = scala.Predef.String
  type Arr[T] = scala.Array[T]
  val Arr = scala.Array

  def !![F[+_]: MonadErr, T](msg: Str): F[T] = new IllegalArgumentException(msg).raiseError[F, T]

  case class V1(
    refCount: Int,
    msgs: Vector[Msg]
  )
  extends Header
  object V1 {
    def apply[F[+_]: MonadErr]()(implicit b: Buffer[F]): F[V1] = {
      import b._
      for {
        _ ← expect("version", 1 toByte)
        _ ← zero("reserved")
        numMsgs ← unsignedShort()
        refCount ← signedInt("refCount")
        size ← unsignedInt()
        msgs ← b.takeBytes(size) { implicit b ⇒ Msg[F] }
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
  extends Header
  object V2 {
    def apply[F[+_]: MonadErr](implicit b: Buffer[F]): F[V2] = {
      import b._
      for {
        _ ← expect("signature", Arr[Byte]('O', 'H', 'D', 'R'))
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
            case 0 ⇒ unsignedByte ().map { _.toLong }
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
          msgs
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
      import b._
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
              case  3 ⇒ Datatype[F]
              case  4 ⇒ FillValue.Old[F]
              case  5 ⇒ FillValue[F]
              case  6 ⇒ Link[F]
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
        import b._
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
          import b._
          for {
            rank ← unsignedByte()
            flags ← byte()
            _ ← expect("reserved", 0, 5)
            dimensions ← Dimensions(rank, flags)
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
          import b._
          for {
            rank ← unsignedByte()
            flags ← byte()
            tpe ← byte()
            msg ←
              tpe match {
                case 0 ⇒ Scalar.pure[F]
                case 1 ⇒ Dimensions(rank, flags).map {Simple(_) }
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
          import b._
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
                case Some(maxs) ⇒
                  sizes
                    .zip(maxs)
                    .map {
                      case (size, max) ⇒ Dimension(size, Some(max))
                    }
                    .pure[F]
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
        import b._
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

    sealed trait Datatype extends Msg
    object Datatype {

      sealed trait Version
      object Version {
        def apply[F[+_]: MonadErr](n: Int): F[Version] =
          n match {
            case 1 ⇒ `1`.pure[F]
            case 2 ⇒ `2`.pure[F]
            case 3 ⇒ `3`.pure[F]
            case n ⇒ !!(s"Invalid datatype version: $n")
          }
        case object `1` extends Version
        case object `2` extends Version
        case object `3` extends Version
      }

      def apply[F[+_]: MonadErr](implicit b: Buffer[F]): F[Datatype] = {
        import b._
        for {
          bits ← byte()
          cls = bits & 0xf
          version ← Version((bits & 0xf0) >> 4)
          flagBytes ← b.getLong(3)
          flags = fromBitMask(Arr(flagBytes))
          size ← signedInt("size")
          datatype ← cls match {
            case  0 ⇒    FixedPoint(flags)
            case  1 ⇒ FloatingPoint(flags)
            case  2 ⇒          Time(flags)
            case  3 ⇒        String(flags)
            case  4 ⇒      Bitfield(flags)
            case  5 ⇒        Opaque(flags)
            case  6 ⇒      Compound(flags, version, size)
            case  7 ⇒     Reference(flags)
            case  8 ⇒   Enumeration(flags, version)
            case  9 ⇒     VarLength(flags)
            case 10 ⇒         Array(flags, version)
            case  n ⇒ err("datatype", n, rewind = 7)
          }
        } yield
          datatype
      }

      case class FixedPoint(
        order: ByteOrder,
        pad: FixedPoint.Pad,
        signed: Boolean,
        offset: Short,
        precision: Short
      )
      extends Datatype
      object FixedPoint {
        case class Pad(lo: Boolean, hi: Boolean)
        def apply[F[+_]: MonadErr](flags: BitSet)(implicit b: Buffer[F]): F[FixedPoint] = {
          import b._
          val order = if (flags(0)) LITTLE_ENDIAN else BIG_ENDIAN
          val pad = Pad(flags(1), flags(2))
          val signed = flags(3)
          for {
            offset ← b.getShort
            precision ← b.getShort
          } yield
            FixedPoint(
              order,
              pad,
              signed,
              offset,
              precision
            )
        }
      }

      case class FloatingPoint(
        pad: FloatingPoint.Pad,
        order: ByteOrder,
        mantissaNormalization: MantissaNormalization,
        bitOffset: Short,
        bitPrecision: Short,
        signPos: Byte,
        exponentPos: Byte,
        exponentSize: Byte,
        mantissaPos: Byte,
        mantissaSize: Byte,
      )
      extends Datatype
      object FloatingPoint {
        case class Pad(lo: Boolean, hi: Boolean, in: Boolean)
        sealed trait MantissaNormalization
        object MantissaNormalization {
          def apply[F[+_]: MonadErr](bit4: Boolean, bit5: Boolean): F[MantissaNormalization] =
            (bit5, bit4) match {
              case (false, false) ⇒    None.pure[F]
              case (false,  true) ⇒     Set.pure[F]
              case ( true, false) ⇒ Implied.pure[F]
              case ( true,  true) ⇒ !!(s"Reserved mantissa-normalization bit flags: {$bit4, $bit5}")
            }
          case object    None extends MantissaNormalization
          case object     Set extends MantissaNormalization
          case object Implied extends MantissaNormalization
        }
        def apply[F[+_]: MonadErr](flags: BitSet)(implicit b: Buffer[F]): F[FloatingPoint] = {
          import b._
          val pad = Pad(flags(1), flags(2), flags(3))
          for {
            order ←
              (flags(6), flags(0)) match {
                case (false, false) ⇒ LITTLE_ENDIAN.pure[F]
                case (false,  true) ⇒    BIG_ENDIAN.pure[F]
                case ( true, false) ⇒ err(s"Invalid endianness bits: {true, false}", flags)
                case ( true,  true) ⇒ err(s"VAX-endianness unsupported", flags)
              }
            signPos = flags.byte(0, 8)
            mantissaNormalization ← MantissaNormalization(flags(5), flags(4))
            offset ← b.getShort
            precision ← b.getShort
            expLoc ← byte()
            expSize ← byte()
            mantissaLoc ← byte()
            mantissaSize ← byte()
          } yield
            FloatingPoint(
              pad,
              order,
              mantissaNormalization,
              offset,
              precision,
              signPos,
              expLoc,
              expSize,
              mantissaLoc,
              mantissaSize
            )
        }
      }

      case class Time(
        order: ByteOrder,
        precision: Int
      )
      extends Datatype
      object Time {
        def apply[F[+_]: MonadErr](flags: BitSet)(implicit b: Buffer[F]): F[Time] = {
          import b._
          val order = if (flags(0)) LITTLE_ENDIAN else BIG_ENDIAN
          for {
            precision ← signedInt("precision")
          } yield
            Time(
              order,
              precision
            )
        }
      }

      case class String(
        pad: String.Pad,
        charset: Charset
      )
      extends Datatype
      object String {
        def apply[F[+_]: MonadErr](flags: BitSet)(implicit b: Buffer[F]): F[String] = {
          import b._
          for {
                pad ←     Pad(flags.byte(0, 4))
            charset ← Charset(flags.byte(4, 8))
          } yield
            String(
              pad,
              charset
            )
        }

        sealed trait Pad
        object Pad {
          def apply[F[+_]: MonadErr](flags: Byte): F[Pad] =
            flags match {
              case 0 ⇒ NullTerminated.pure[F]
              case 1 ⇒     NullPadded.pure[F]
              case 2 ⇒    SpacePadded.pure[F]
              case n ⇒ !!(s"Invalid string pad enum: $n")
            }
          case object NullTerminated extends Pad
          case object     NullPadded extends Pad
          case object    SpacePadded extends Pad
        }
      }

      case class Bitfield(
        order: ByteOrder,
        pad: Bitfield.Pad,
        offset: Short,
        precision: Short
      )
      extends Datatype
      object Bitfield {
        case class Pad(lo: Boolean, hi: Boolean)
        def apply[F[+_]: MonadErr](flags: BitSet)(implicit b: Buffer[F]): F[Bitfield] = {
          import b._
          val order = if (flags(0)) LITTLE_ENDIAN else BIG_ENDIAN
          val pad = Pad(flags(1), flags(2))
          for {
            offset ← b.getShort
            precision ← b.getShort
          } yield
            Bitfield(
              order,
              pad,
              offset,
              precision
            )
        }
      }

      case class Opaque(
        description: Str
      )
      extends Datatype
      object Opaque {
        def apply[F[+_]: MonadErr](flags: BitSet)(implicit b: Buffer[F]): F[Opaque] = {
          val length = flags.byte(0, 8)
          for {
            description ← b.ascii
            _ ← {
              if (length == roundUp(description.length, 8))
                (()).pure[F]
              else
                b.err(s"'Opaque' header message description has length ${description.length}, expected to round up by 8 to $length: $description", length, rewind = description.length)
            }
          } yield
            new Opaque(description)
        }
      }

      case class Compound(
        types: Vector[Compound.Elem]
      )
      extends Datatype
      object Compound {
        case class Elem(name: Str, offset: Int, datatype: Datatype)
        def apply[F[+_]: MonadErr](flags: BitSet, version: Version, size: Int)(implicit b: Buffer[F]): F[Compound] = {
          import b._
          import Version._
          val num = flags.int(0, 16)
          for {
            datatypes ← takeN(num) {
              implicit b ⇒ import b._
              for {
                name ←
                  version match {
                    case `1` | `2` ⇒ b.ascii(8)
                    case `3` ⇒ b.ascii
                  }
                offset ←
                  version match {
                    case `1` | `2` ⇒ signedInt("elem offset")
                    case `3` ⇒ b.getInt(
                      if (size < (1 <<  8)) 1 else
                      if (size < (1 << 16)) 2 else
                      if (size < (1 << 24)) 3 else
                                            4
                    )
                  }
                _ ←
                  version match {
                    case `1` ⇒
                      for {
                        dimensionality ← unsignedByte()
                        _ ← expect("reserved", 0 toByte, 3)
                        _ ← expect("permutation idxs", 0 toByte, 4)
                        _ ← expect("reserved", 0 toByte, 4)
                        dimensions ← takeN(dimensionality) {
                          implicit b ⇒ import b._
                            unsignedInt()
                        }
                      } yield
                        ()
                    case `2` | `3` ⇒ (()).pure[F]
                  }
                datatype ← Datatype[F]
              } yield
                Elem(name, offset, datatype)
            }
          } yield
            Compound(
              datatypes
            )
        }
      }

      sealed trait Reference extends Datatype
      object Reference {
        case object Object extends Reference
        case object Region extends Reference

        def apply[F[+_]: MonadErr](flags: BitSet)(implicit b: Buffer[F]): F[Reference] = {
          import b._
          flags.byte(0, 4) match {
            case 0 ⇒ Object.pure[F]
            case 1 ⇒ Region.pure[F]
            case n ⇒ err(s"Reference type", n, rewind = 7)
          }
        }
      }

      case class Enumeration(elems: Vector[Enumeration.Elem]) extends Datatype
      object Enumeration {
        case class Elem(name: Str, value: Int)
        def apply[F[+_]: MonadErr](flags: BitSet, version: Version)(implicit b: Buffer[F]): F[Enumeration] = {
          import b._
          import Version._
          val num = flags.int(0, 16)
          for {
            base ← Datatype[F]
            names ← takeN(num) {
              implicit b ⇒ import b._
              version match {
                case `1` | `2` ⇒ b.ascii(padTo = 8)
                case `3` ⇒ b.ascii
              }
            }
            values ← takeN(num) { implicit b ⇒
              // TODO: verify that `base` is a scalar integer; parse value accordingly. Later: support other types as well
              ??? : F[Int]
            }
            elems =
              names.zip(values).map {
                case (name, value) ⇒
                  Elem(name, value)
              }
          } yield
            Enumeration(elems)
        }
      }

      sealed trait VarLength extends Datatype
      object VarLength {
        case class Sequence(base: Datatype) extends VarLength
        case class Strings(
          pad: String.Pad,
          charset: Charset,
          base: Datatype
        )
        extends VarLength

        def apply[F[+_]: MonadErr](flags: BitSet)(implicit b: Buffer[F]): F[VarLength] = {
          for {
            base ← Datatype[F]
            varlength ←
              flags.int(0, 4) match {
                case 0 ⇒ Sequence(base).pure[F]
                case 1 ⇒
                  for {
                        pad ← String.Pad(flags.byte(4,  8))
                    charset ←    Charset(flags.byte(8, 12))
                  } yield
                    Strings(pad, charset, base)
                case n ⇒ b.err(s"varlength type", n, rewind = 7)
              }
          } yield
            varlength
        }
      }

      case class Array(base: Datatype, dimensions: Vector[Long]) extends Datatype
      object Array {
        def apply[F[+_]: MonadErr](flags: BitSet, version: Version)(implicit b: Buffer[F]): F[Array] = {
          import b._
          import Version._
          version match {
            case `1` ⇒ err(s"datatype version for array", 1, rewind = 7)
            case `2` ⇒
              for {
                dimensionality ← unsignedByte()
                _ ← expect("reserved", 0 toByte, 3)
                     dimensions ← takeN(dimensionality) { _.unsignedInt() }
                permutationIdxs ← takeN(dimensionality) { _.unsignedInt() }
                _ ←
                  permutationIdxs
                    .zipWithIndex
                    .map {
                      case (permutationIdx, idx) ⇒
                        if (permutationIdx == idx)
                          (()).pure[F]
                        else
                          err(s"Non-identity permutation", (permutationIdx, idx), rewind = 4 * (dimensionality - idx))
                    }
                    .sequence
                base ← Datatype[F]
              } yield
                Array(base, dimensions)
            case `3` ⇒
              for {
                dimensionality ← unsignedByte()
                dimensions ← takeN(dimensionality) {
                  implicit b ⇒ import b._
                    unsignedInt()
                }
                base ← Datatype[F]
              } yield
                Array(base, dimensions)
          }
        }
      }
    }

    case class FillValue(
      allocationTime: AllocationTime,
      writeTime: WriteTime,
      value: ?[Arr[Byte]]
    )
    extends Msg
    object FillValue {
      sealed trait AllocationTime
      object AllocationTime {
        case object       Early extends AllocationTime
        case object        Late extends AllocationTime
        case object Incremental extends AllocationTime

        def apply[F[+_]: MonadErr](n: Int): F[AllocationTime] =
          n match {
            case 1 ⇒       Early.pure[F]
            case 2 ⇒        Late.pure[F]
            case 3 ⇒ Incremental.pure[F]
            case n ⇒ !!(s"Invalid fill-value version: $n")
          }
      }

      sealed trait WriteTime
      object WriteTime {
        case object OnAllocation extends WriteTime
        case object        Never extends WriteTime
        case object     UserOnly extends WriteTime

        def apply[F[+_]: MonadErr](n: Int): F[WriteTime] =
          n match {
            case 0 ⇒ OnAllocation.pure[F]
            case 1 ⇒        Never.pure[F]
            case 2 ⇒     UserOnly.pure[F]
            case n ⇒ !!(s"Invalid fill-value version: $n")
          }
      }

      sealed trait Version
      object Version {
        def apply[F[+_]: MonadErr](n: Int): F[Version] =
          n match {
            case 1 ⇒ `1`.pure[F]
            case 2 ⇒ `2`.pure[F]
            case 3 ⇒ `3`.pure[F]
            case n ⇒ !!(s"Invalid fill-value version: $n")
          }
        case object `1` extends Version
        case object `2` extends Version
        case object `3` extends Version
      }

      def apply[F[+_]: MonadErr](implicit b: Buffer[F]): F[FillValue] = {
        import b._
        import Version._
        for {
          versionByte ← byte()
          version ← Version(versionByte)
          t ←
            version match {
              case `1` | `2` ⇒
                for {
                  allocationTime ← byte().>>= { AllocationTime[F](_) }
                       writeTime ← byte().>>= {      WriteTime[F](_) }
                  isDefined ← byte().>>= {
                    case 0 ⇒ false.pure[F]
                    case 1 ⇒  true.pure[F]
                    case n ⇒ err("fill value defined", n, rewind = 1)
                  }
                } yield
                  (allocationTime, writeTime, isDefined)
              case `3` ⇒
                for {
                  flags ← byte()
                  allocationTime ← AllocationTime(flags & 0x3)
                       writeTime ← WriteTime(flags & 0xc)
                       isDefined ←
                        ((flags & 0x10) > 0, (flags & 0x10) > 0) match {
                          case ( true, false) ⇒ false.pure[F]
                          case (false,  true) ⇒  true.pure[F]
                          case (bit4, bit5) ⇒ err("fill-value undefined and defined bits are equal", (bit4, bit5))
                        }
                } yield
                  (allocationTime, writeTime, isDefined)
            }
          (allocationTime, writeTime, isDefined) = t
          value ←
            version match {
              case `1` ⇒
                for {
                  size ← signedInt("fill value size")
                  value ← bytes("fill value", size)
                } yield
                  isDefined ? value
              case `2` | `3` ⇒
                {
                  isDefined ? {
                    for {
                      size ← signedInt("fill value size")
                      value ← bytes("fill value", size)
                    } yield
                      value
                  }
                }
                .sequence
            }
        } yield
          FillValue(
            allocationTime,
            writeTime,
            value
          )
      }

      case class Old(value: Arr[Byte]) extends Msg
      object Old {
        def apply[F[+_]: MonadErr](implicit b: Buffer[F]): F[Old] = {
          import b._
          for {
            size ← signedInt("size")
            bytes ← bytes("bytes", size)
          } yield
            Old(bytes)
        }
      }
    }

    sealed trait Link extends Msg
    object Link {
      case class     Hard(name: String, creationOrder: ?[Long], addr: Addr           ) extends Link
      case class     Soft(name: String, creationOrder: ?[Long], dest:  Str           ) extends Link
      case class External(name: String, creationOrder: ?[Long], file:  Str, dest: Str) extends Link

      sealed trait Type
      object Type {
        case object     Hard extends Type
        case object     Soft extends Type
        case object External extends Type
      }

      def apply[F[+_]: MonadErr](implicit b: Buffer[F]): F[Link] = {
        import b._
        for {
          _ ← expect("version", 1 toByte)
          flags ← byte()
          tpe ← {
            if ((flags & 0x8) == 0)
              Type.Hard.pure[F]
            else
              for {
                tpe ← byte()
              } yield
                tpe match {
                  case 1 ⇒ Type.Soft
                  case 64 ⇒ Type.External
                  case n ⇒ err(s"link type", n)
                }
          }
          creationOrder ←
            {
              ((flags & 0x4) > 0) ? {
                unsignedLong("creation order")
              }
            }
            .sequence
          charset ← {
            if ((flags & 0x10) == 0)
              Charset.Ascii.pure[F]
            else
              for {
                _ ← expect("utf-8", 1 toByte)
              } yield
                Charset.Utf8
          }
          nameBytesSize = 1 << (flags & 0x3)
          nameBytes ← bytes("link-name bytes", nameBytesSize)
          name = charset.decode(nameBytes)
          link ←
            tpe match {
              case Type.Hard ⇒
                for {
                  addr ← offset("hard-link addr")
                } yield
                  Hard(name, creationOrder, addr)
              case Type.Soft ⇒
                for {
                  length ← unsignedShort()
                  bytes ← b.bytes("soft-link name", length)
                } yield
                  Soft(
                    name,
                    creationOrder,
                    charset.decode(bytes)
                  )
              case Type.External ⇒
                for {
                  length ← unsignedShort()
                  _ ← zero("external-link flags")
                  bytes ← b.bytes("extenral-link info", length - 1)
                  buf = ByteBuffer.wrap(bytes).order(LITTLE_ENDIAN)
                  chars =
                    charset
                      .cs
                      .decode(buf)
                      .array
                  strings ←
                    chars
                      .zipWithIndex
                      .collect {
                        case (ch, idx) if ch == 0.toByte ⇒ idx
                      } match {
                        case Array(nul) ⇒
                          (
                            chars.take(nul    ).mkString,
                            chars.drop(nul + 1).mkString
                          )
                          .pure[F]
                        case nulls ⇒
                          err(s"", chars.mkString, rewind = bytes.length)
                      }
                  (file, path) = strings
                } yield
                  External(
                    name,
                    creationOrder,
                    file,
                    path
                  )
            }
        } yield
          link
      }
    }

    sealed trait DataLayout extends Msg
    object DataLayout {
      case class    Compact(data: Array[Byte], dimensions: ?[Vector[Long]] = None) extends DataLayout
      sealed abstract class Contiguous(val addr: Addr) extends DataLayout
      object Contiguous {
        case class V12(override val addr: Addr, dimensions: Vector[Long]) extends Contiguous(addr)
        case class V3 (override val addr: Addr,           length: Length) extends Contiguous(addr)
      }
      case class    Chunked(addr: Addr, dimensions: Vector[Long], elemSize: Int) extends DataLayout

      sealed trait Class
      object Class {
        case object    Compact extends Class
        case object Contiguous extends Class
        case object    Chunked extends Class

        def apply[F[+_]: MonadErr](n: Int): F[Class] =
          n match {
            case 0 ⇒    Compact.pure[F]
            case 1 ⇒ Contiguous.pure[F]
            case 2 ⇒    Chunked.pure[F]
            case n ⇒ !!(s"Invalid Data Layout class: $n")
          }
      }

      def apply[F[+_]: MonadErr](implicit b: Buffer[F]): F[DataLayout] = {
        import b._
        for {
          version ← byte()
          layout ←
            version match {
              case 1 | 2 ⇒
                for {
                  dimensionality ← unsignedByte()
                  layoutByte ← byte()
                  clz ← Class(layoutByte)
                  _ ← expect("reserved", 0 toByte, 5)
                  datalayout ←
                    clz match {
                      case Class.Compact ⇒
                        for {
                          dimensions ← takeN(dimensionality) { _.getLong }
                          size ← signedInt("compact data size")
                          data ← bytes("compact data bytes", size)
                        } yield
                          Compact(data, Some(dimensions))
                      case Class.Contiguous ⇒
                        for {
                          addr ← offset("data address")
                          dimensions ← takeN(dimensionality) { _.getLong }
                        } yield
                          Contiguous.V12(
                            addr,
                            dimensions
                          )
                      case Class.Chunked ⇒
                        for {
                          addr ← offset("data address")
                          dimensions ← takeN(dimensionality - 1) { _.getLong }
                          elemSize ← signedInt("chunked data elem size")
                        } yield
                          Chunked(
                            addr,
                            dimensions,
                            elemSize
                          )
                    }
                } yield
                  datalayout
              case 3 ⇒
                for {
                  layoutByte ← byte()
                  clz ← Class(layoutByte)
                  datalayout ←
                    clz match {
                      case Class.Compact ⇒
                        for {
                          size ← unsignedShort()
                          data ← bytes("compact data", size)
                        } yield
                          Compact(data)
                      case Class.Contiguous ⇒
                        for {
                          addr ← offset("contiguous data offset")
                           len ← length("contiguous data length")
                        } yield
                          Contiguous.V3(addr, len)
                      case Class.Chunked ⇒
                        for {
                          dimensionality ← unsignedByte()
                          addr ← offset("chunked data")
                          dimensions ← takeN(dimensionality) { _.getLong }
                          elemSize ← signedInt("chunked data elem size")
                        } yield
                          Chunked(
                            addr,
                            dimensions,
                            elemSize
                          )
                    }
                } yield
                  datalayout
              case n ⇒ err(s"data layout version", n, rewind = 1)
            }
        } yield
          layout
      }
    }

    case class Comment(msg: String) extends Msg
    object Comment {
      def apply[F[+_]: MonadErr](implicit b: Buffer[F]): F[Comment] =
        b.ascii.map(Comment(_))
    }
  }

  sealed abstract class Charset(val cs: java.nio.charset.Charset) {
    def decode(buf: ByteBuffer): String = cs.decode(buf).toString
    def decode(bytes: Arr[Byte]): String = decode(ByteBuffer.wrap(bytes).order(LITTLE_ENDIAN))
  }
  object Charset {
    def apply[F[+_]: MonadErr](flags: Byte): F[Charset] =
      flags match {
        case 0 ⇒ Ascii.pure[F]
        case 1 ⇒  Utf8.pure[F]
        case n ⇒ !!(s"Invalid charset enum: $n")
      }

    case object  Utf8 extends Charset(UTF_8)
    case object Ascii extends Charset(US_ASCII)
  }

  implicit class BitSetOps(val bitSet: BitSet) extends AnyVal {
    def byte(start: Int, end: Int): Byte =
      if (end - start >= 8 || start > end)
        throw new IllegalArgumentException(s"Slicing ${end - start} bytes (≥8) as byte unsupported")
      else
        (start until end).map {
          i ⇒
            {
              if (bitSet(i))
                1 << i
              else
                0
            }
            .toByte
        }
        .sum

    def short(start: Int, end: Int): Short =
      if (end - start >= 16 || start > end)
        throw new IllegalArgumentException(s"Slicing ${end - start} bytes (≥16) as short unsupported")
      else
        (start until end).map {
          i ⇒
            {
              if (bitSet(i))
                1 << i
              else
                0
            }
            .toShort
        }
        .sum

    def int(start: Int, end: Int): Int =
      if (end - start >= 32 || start > end)
        throw new IllegalArgumentException(s"Slicing ${end - start} bytes (≥32) as int unsupported")
      else
        (start until end).map {
          i ⇒
            {
              if (bitSet(i))
                1 << i
              else
                0
            }
        }
        .sum
  }
}
