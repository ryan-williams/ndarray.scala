package org.lasersonlab.hdf5.io

import java.nio.ByteBuffer

import hammerlab.either._
import org.lasersonlab.hdf5.io.Buffer.UnsupportedValue

import Array.fill

case class Buffer(buf: ByteBuffer) {

  type Verified[T] = UnsupportedValue[T] | Unit

  def offset(name: String): UnsupportedValue[Long] | Long = {
    val long = buf.getLong
    if (long < 0)
      L(UnsupportedValue(name, long, buf.position() - 8))
    else
      R(long)
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

  def undefined(name: String) = expect(name, 0xff toByte, 8)
}
object Buffer {
  case class UnsupportedValue[T](name: String, value: T, offset: Int) extends Exception
  implicit def wrap(buf: ByteBuffer) = Buffer(buf)
}
