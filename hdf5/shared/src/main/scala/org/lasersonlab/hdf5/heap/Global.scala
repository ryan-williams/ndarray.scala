package org.lasersonlab.hdf5.heap

import cats.implicits._
import hammerlab.collection._
import hammerlab.either._
import hammerlab.math.utils._
import org.lasersonlab.hdf5.{ Addr, Length }
import org.lasersonlab.hdf5.io.Buffer
import org.lasersonlab.hdf5.io.Buffer.UnsupportedValue
import Global.Object

case class Global(objects: Vector[Object])
object Global {
  def apply()(implicit b: Buffer) = {
    import b._
    val start = b.position()
    for {
      _ ← expect("magic", Array[Byte]('G', 'C', 'O', 'L'))
      _ ← expect("version", 1 toByte)
      _ ← expect("reserved", Array[Byte](0, 0, 0))
      size ← length("size")
      end = start + size
      _ = b.buf.limit()
      objects ←
        b.position().unfoldLeft {
          pos ⇒
            if (pos < end) {
              Some((Object(), b.position()))
            } else
              None
        }
        .sequence
        .map { _.toVector }
    } yield {
      b.buf.position(end)
      Global(objects)
    }
  }

  case class Object(
    id: Int,
    refCount: Int,
    size: Length,
    data: Array[Byte]
  )
  object Object {
    def apply()(implicit b: Buffer): Exception | Object = {
      import b._
      val id = unsignedShort()
      val refCount = unsignedShort()
      for {
        _ ← expect("reserved", Array[Byte](0, 0, 0, 0))
        size ← length("size")
        size ← size.safeInt
        data = bytes("data", size)
        _ = {
          bytes("data padding", (8 - size % 8) % 8)
        }
      } yield
        Object(id, refCount, size, data)
    }
  }

  case class ID(heap: Addr, idx: Long)
  object ID {
    def apply()(implicit b: Buffer): UnsupportedValue[Long] | ID = {
      import b._
      for {
        heap ← offset("heap")
        idx = unsignedInt()
      } yield
        ID(heap, idx)
    }
  }
}
