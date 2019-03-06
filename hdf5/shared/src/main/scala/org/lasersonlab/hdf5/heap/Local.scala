package org.lasersonlab.hdf5.heap

import cats.implicits._
import org.lasersonlab.hdf5.io.Buffer
import org.lasersonlab.hdf5.io.Buffer.{ MonadErr, UnsupportedValue, syntax }
import org.lasersonlab.hdf5.{ Addr, Length }

case class Local(
  size: Length,
  free: Length,
  data: Addr
)
object Local {
  def apply[F[+_]: MonadErr]()(implicit b: Buffer[F]): F[Local] = {
    val s = syntax(b); import s._
    for {
      _ ← expect("magic", Array[Byte]('H', 'E', 'A', 'P'))
      _ ← expect("version", 0 toByte)
      _ ← expect("reserved", Array[Byte](0, 0, 0))
      size ← length("data segment size")
      free ← length("free-list head-offset")
      data ← offset("data segment")
    } yield
      Local(size, free, data)
  }
}
