package org.lasersonlab.hdf5.heap

import cats.implicits._
import hammerlab.collection._
import hammerlab.math.utils._
import org.lasersonlab.hdf5.heap.Global.Object
import org.lasersonlab.hdf5.io.Buffer
import org.lasersonlab.hdf5.io.Buffer.{ EOFException, MonadErr, syntax }
import org.lasersonlab.hdf5.{ Addr, Length }

case class Global(objects: Vector[Object])
object Global {
  def apply[F[+_]: MonadErr]()(implicit b: Buffer[F]): F[Global] = {
    val s = syntax(b); import s._
    for {
      start ← b.position
      _ ← expect("magic", Array[Byte]('G', 'C', 'O', 'L'))
      _ ← expect("version", 1 toByte)
      _ ← expect("reserved", Array[Byte](0, 0, 0))
      size ← length("size")
      pos ← b.position
      end = start + size
      objects ← b.takeUntil(end - pos) {
        implicit b ⇒ Object[F]()
      }
    } yield {
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
    def apply[F[+_]: MonadErr]()(implicit b: Buffer[F]): F[Object] = {
      val s = syntax(b); import s._
      val F = MonadErr[F]
      for {
        id ← unsignedShort()
        refCount ← unsignedShort()
        _ ← expect("reserved", Array[Byte](0, 0, 0, 0))
        size ← length("size")
        size ← F.rethrow { size.safeInt.pure[F] }
        data ← bytes("data", size)
        _ ← bytes("data padding", (8 - size % 8) % 8)
      } yield
        Object(id, refCount, size, data)
    }
  }

  case class ID(heap: Addr, idx: Long)
  object ID {
    def apply[F[+_]: MonadErr]()(implicit b: Buffer[F]): F[ID] = {
      val s = syntax(b); import s._
      for {
        heap ← offset("heap")
        idx ← unsignedInt()
      } yield
        ID(heap, idx)
    }
  }
}
