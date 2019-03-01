package org.lasersonlab.hdf5.io

import java.nio.ByteBuffer

import cats.implicits._
import hammerlab.collection._
import hammerlab.either._
import hammerlab.option._
import org.lasersonlab.hdf5.{ Addr, Length }
import org.lasersonlab.hdf5.io.Buffer.UnsupportedValue

import Array.fill

case class Buffer(buf: ByteBuffer) {

  type Verified[T] = UnsupportedValue[T] | Unit

  def length(name: String): UnsupportedValue[Long] | Length = offset(name)

  def offset(name: String): UnsupportedValue[Long] | Addr = {
    val long = buf.getLong
    if (long < 0)
      L(UnsupportedValue(name, long, buf.position() - 8))
    else
      R(long)
  }

  def offset_?(name: String): UnsupportedValue[Long] | ?[Addr] = {
    val long = buf.getLong
    if (long == -1)
      R(None)
    if (long < 0)
      L(UnsupportedValue(name, long, buf.position() - 8))
    else
      R(Some(long))
  }

  def int(name: String, expected: Int) = {
    val int = buf.getInt
    if (int != expected)
      L(UnsupportedValue(name, int, buf.position() - 4))
    else
      R(())
  }

  def expect(name: String, value: Byte): Verified[Byte] = {
    val byte = buf.get()
    if (byte != 0)
      L(UnsupportedValue(name, byte, buf.position() - 1))
    else
      R(())
  }

  def expect(name: String, expected: Array[Byte]): Verified[Array[Byte]] = {
    val actual = fill(expected.length)(0 toByte)
    buf.get(actual)
    if (!actual.sameElements(expected))
      L(UnsupportedValue(name, actual, buf.position() - 1))
    else
      R(())
  }

  def expect(name: String, value: Byte, num: Int): Verified[Byte] = {
    if (num == 0) R(())
    else
      for {
        _ ← zero(name)
        rest ← expect(name, value, num - 1)
      } yield
        rest
  }

  def zero(name: String) = expect(name, 0 toByte)

  def unsignedShort(): Int = {
    val short = buf.getShort
    if (short < 0)
      short.toInt + 0xffff
    else
      short
  }

  def unsignedByte(): Short = {
    val byte = buf.get()
    if (byte < 0)
      (byte + 0xff).toShort
    else
      byte
  }

  def unsignedInt(): Long = {
    val int = buf.getInt()
    if (int < 0)
      int + 0xffffffffL
    else
      int
  }

  def unsignedLongs(name: String): UnsupportedValue[(Int, Long)] | Vector[Long] = {
    val start = buf.position()
    0.unfoldLeft[UnsupportedValue[(Int, Long)] | Long] {
      idx ⇒
        val long = buf.getLong()
        if (long < 0)
          Some(L(UnsupportedValue(name, (idx, long), start)) → (idx + 1))
        else if (long == 0)
          None
        else
          Some(R(long) → (idx + 1))
    }
    .toVector
    .sequence
  }

  def undefined(name: String) = expect(name, 0xff toByte, 8)

  def bytes(name: String, size: Int): Array[Byte] = {
    val arr = fill[Byte](size)(0)
    buf.get(arr)
    arr
  }
}
object Buffer {
  case class UnsupportedValue[T](name: String, value: T, offset: Int) extends Exception
  implicit def wrap(buf: ByteBuffer) = Buffer(buf)
  implicit def wrap(buf: Buffer): ByteBuffer = buf.buf
}
