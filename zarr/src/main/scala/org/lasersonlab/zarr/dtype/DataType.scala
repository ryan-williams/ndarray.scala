package org.lasersonlab.zarr.dtype

import java.nio.ByteBuffer
import java.nio.ByteBuffer._

import cats.implicits._
import io.circe.DecodingFailure.fromThrowable
import io.circe._
import lasersonlab.xscala._
import org.lasersonlab.zarr
import org.lasersonlab.zarr.dtype.DataType._
import org.lasersonlab.zarr.|
import shapeless._

import scala.collection.immutable.ListMap
import scala.util.Try

/**
 * Representation of a Zarr record-type; includes functionality for IO to and from a [[ByteBuffer]]
 *
 * Subclasses:
 *
 * - [[Primitive]]s:
 *   - Integer types: [[byte]], [[short]], [[int]], [[long]]
 *   - Floating-point types: [[float]], [[double]]
 *   - [[string]]
 * - Structs:
 *   - [[Struct "typed"]]: auto-derived for a case-class
 *   - [[untyped.Struct "untyped"]]: "bag of fields", corresponding to [[org.lasersonlab.zarr.untyped.Struct]]
 *
 * "Typed" and "Untyped" structs can represent the same logical underlying datatype (e.g. the JSON representation, in
 * [[org.lasersonlab.zarr.Metadata array metadata]]'s "dtype" field will be the same), but allow for call-sites that
 * know/enforce a more structured schema vs. not
 */
sealed trait DataType {
  def size: Int
  type T
  def apply(buff: ByteBuffer): T
  def  read(buff: ByteBuffer, idx: Int): T = {
    buff.position(size * idx)
    apply(buff)
  }
  def apply(buffer: ByteBuffer, t: T): Unit
  def apply(t: T): Array[Byte] = {
    val buff = allocate(size)
    apply(buff, t)
    buff.array()
  }
  def t: Aux[T] = this
}

object DataType
  extends StructDerivations
     with EqInstances
     with Coders {

  type Aux[_T] = DataType { type T = _T }

  /**
   * Common interface for non-struct datatypes (numerics, strings)
   */
  sealed abstract class Primitive[_T](
    val order: ByteOrder,
    val dType: DType,
    val size: Int
  ) extends DataType {
    type T = _T
    override val toString = s"$order$dType$size"
  }

  import ByteOrder._
  type Order = ByteOrder

  import org.lasersonlab.zarr.dtype.{ DType ⇒ d }

  val `0` = 0.toByte

  // TODO: setting the buffer's order every time seems suboptimal; some different design that streamlines that would be nice
  case object   byte                                 extends Primitive[  Byte]( None, d.   int,    1) { @inline def apply(buf: ByteBuffer): T = {                   buf.get       }; @inline override def apply(b: ByteBuffer, t: T) = b             .put      (t) }
  case  class  short(override val order: Endianness) extends Primitive[ Short](order, d.   int,    2) { @inline def apply(buf: ByteBuffer): T = { buf.order(order); buf.getShort  }; @inline override def apply(b: ByteBuffer, t: T) = b.order(order).putShort (t) }
  case  class    int(override val order: Endianness) extends Primitive[   Int](order, d.   int,    4) { @inline def apply(buf: ByteBuffer): T = { buf.order(order); buf.getInt    }; @inline override def apply(b: ByteBuffer, t: T) = b.order(order).putInt   (t) }
  case  class   long(override val order: Endianness) extends Primitive[  Long](order, d.   int,    8) { @inline def apply(buf: ByteBuffer): T = { buf.order(order); buf.getLong   }; @inline override def apply(b: ByteBuffer, t: T) = b.order(order).putLong  (t) }
  case  class  float(override val order: Endianness) extends Primitive[ Float](order, d. float,    4) { @inline def apply(buf: ByteBuffer): T = { buf.order(order); buf.getFloat  }; @inline override def apply(b: ByteBuffer, t: T) = b.order(order).putFloat (t) }
  case  class double(override val order: Endianness) extends Primitive[Double](order, d. float,    8) { @inline def apply(buf: ByteBuffer): T = { buf.order(order); buf.getDouble }; @inline override def apply(b: ByteBuffer, t: T) = b.order(order).putDouble(t) }
  case  class string(override val  size:        Int) extends Primitive[String]( None, d.string, size) {
    import scala.Array.fill
    val arr = fill(size)(`0`)
    def apply(buf: ByteBuffer): T = {
      buf.get(arr)
      val builder = new StringBuilder
      var i = 0
      while (i < size) {
        val char = arr(i)
        if (char != `0`) builder += char.toChar
        else return builder.result()
        i += 1
      }
      builder.result()
    }

    val maxValue = Byte.MaxValue.toChar

    def apply(buffer: ByteBuffer, t: String): Unit = {
      var i = 0
      while (i < t.length) {
        val ch = t(i)
        if (ch > maxValue)
          throw new IllegalArgumentException(
            s"Invalid character in string $t at position $i: $ch"
          )
        buffer.put(ch.toByte)
        i += 1
      }
      while (i < size) {
        buffer.put(`0`)
        i += 1
      }
    }
  }

  type byte = byte.type

  /**
   * Construct primitive data-types, in the presence of an implicit [[Endianness]], either from an un-applied reference
   * to the companion object (e.g. `float` instead of `float(LittleEndian)`)
   */

  type <>! = Endianness

  object  short { implicit def apply(v:  short.type)(implicit e: <>!): Aux[ Short] =  short(e) }
  object    int { implicit def apply(v:    int.type)(implicit e: <>!): Aux[   Int] =    int(e) }
  object   long { implicit def apply(v:   long.type)(implicit e: <>!): Aux[  Long] =   long(e) }
  object  float { implicit def apply(v:  float.type)(implicit e: <>!): Aux[ Float] =  float(e) }
  object double { implicit def apply(v: double.type)(implicit e: <>!): Aux[Double] = double(e) }

  /**
   * Expose implicit [[Primitive]] instances for derivations (assuming sufficient [[Endianness]] evidence)
   */

  implicit val   _byte                               =   byte
  implicit def  _short(implicit e: <>!): Aux[ Short] =  short(e)
  implicit def    _int(implicit e: <>!): Aux[   Int] =    int(e)
  implicit def   _long(implicit e: <>!): Aux[  Long] =   long(e)
  implicit def  _float(implicit e: <>!): Aux[ Float] =  float(e)
  implicit def _double(implicit e: <>!): Aux[Double] = double(e)

  object untyped {
    /**
     * [[zarr.untyped.Struct "Untyped" struct]] [[DataType]]
     *
     * TODO: make this Struct.?
     */
    case class Struct(entries: List[StructEntry])
      extends DataType {
      type T = zarr.untyped.Struct

      val size: Int =
        entries
          .map(_.size)
          .sum

      def apply(buff: ByteBuffer): T =
        zarr.untyped.Struct(
          entries
            .foldLeft(
              ListMap.newBuilder[String, Any]
            ) {
              case (
                builder,
                StructEntry(
                  name,
                  datatype
                )
              ) ⇒
                builder +=
                  name → datatype(buff)
            }
            .result()
        )

      def apply(buffer: ByteBuffer, t: zarr.untyped.Struct): Unit = {
        val es = entries.iterator
        val fields = t.iterator

        while (es.hasNext) {
          val (StructEntry(name, datatype), (k, v)) = (es.next, fields.next)
          if (k != name)
            throw new IllegalStateException(
              s"Incorrect field in struct: [$k,$v] (expected: [$name,$datatype]). Full struct: ${
                t
                  .map { case (k, v) ⇒ s"[$k,$v]" }
                  .mkString(" ")
              }"
            )

          datatype(buffer, v.asInstanceOf[datatype.T])
        }
        if (fields.hasNext)
          throw new IllegalStateException(
            s"Extra fields found in struct: ${
              t
                .map { case (k, v) ⇒ s"[$k,$v]" }
                .mkString(" ")
            } (expected: ${entries.mkString(" ")})"
          )
      }
    }
  }

  case class StructEntry(name: String, datatype: DataType) {
    val size = datatype.size
    override def toString: String =
      Seq(
        name,
        size
      )
      .mkString(
        "[\"",
        "\",\"",
        "\"]"
      )
  }

  case class StructList[L <: HList](
    entries: List[StructEntry],
    size: Int
  )(
    read: ByteBuffer ⇒ L,
    write: (ByteBuffer, L) ⇒ Unit
  ) {
    type T = L
    @inline def apply(buff: ByteBuffer): T = read(buff)
    @inline def apply(buffer: ByteBuffer, t: L): Unit = write(buffer, t)
  }

  case class Struct[
    S,
    L <: HList
  ](
    entries: StructList[L]
  )(
    implicit
    g: LabelledGeneric.Aux[S, L]
  )
  extends DataType {
    val size: Int = entries.size
    override type T = S
    @inline def apply(buffer: ByteBuffer): T = g.from(entries(buffer))
    @inline def apply(buffer: ByteBuffer, t: S): Unit = entries(buffer, g.to(t))
  }

  def get(order: ByteOrder, dtype: DType, size: Int): String | DataType =
    (order, dtype, size) match {
      case (  None, _: d.   int,    1) ⇒ Right(  byte      )
      case (e: <>!, _: d.   int,    2) ⇒ Right( short(   e))
      case (e: <>!, _: d.   int,    4) ⇒ Right(   int(   e))
      case (e: <>!, _: d.   int,    8) ⇒ Right(  long(   e))
      case (e: <>!, _: d. float,    4) ⇒ Right( float(   e))
      case (e: <>!, _: d. float,    8) ⇒ Right(double(   e))
      case (  None, _: d.string, size) ⇒ Right(string(size))
      case _ ⇒
        Left(
          s"Unrecognized data type: $order$dtype$size"
        )
    }

  val regex = """(.)(.)(\d+)""".r
  def get(str: String, c: HCursor): DecodingFailure | DataType =
    for {
      t ←
        Try {
          val regex(order, tpe, size) = str
          (order, tpe, size)
        }
        .toEither
        .left
        .map(
          fromThrowable(_, c.history)
        )
      (order, dtype, size) = t
         order ← ByteOrder.map.get(order).fold[DecodingFailure | ByteOrder] { Left(DecodingFailure(s"Unrecognized order: $order", c.history)) } { Right(_) }
         dtype ←     DType.map.get(dtype).fold[DecodingFailure |     DType] { Left(DecodingFailure(s"Unrecognized dtype: $dtype", c.history)) } { Right(_) }
          size ← Try(size.toInt).toEither.left.map(  fromThrowable(_, c.history))
      datatype ←  get(order, dtype, size).left.map(DecodingFailure(_, c.history))
    } yield
      datatype
}
