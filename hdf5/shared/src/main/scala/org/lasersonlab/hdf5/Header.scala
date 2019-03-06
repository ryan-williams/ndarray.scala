package org.lasersonlab.hdf5

import java.net.URI

import cats.implicits._
import hammerlab.either._
import org.lasersonlab.files.Uri
import org.lasersonlab.hdf5.io.Buffer
import org.lasersonlab.hdf5.io.Buffer.{ EOFException, MonadErr, syntax }

import scala.concurrent.{ ExecutionContext, Future }
import Array.fill

sealed trait Header {
  def base: Long
}

object Header {

  case class V0(
    base: Long,
     eof: Long,
    root: SymbolTable.Entry
  )
  extends Header
  object V0 {
    def apply[F[+_]: MonadErr](implicit b: Buffer[F]): F[V0] = {
      val s = syntax(b); import s._
      for {
        _ ← zero("free space")
        _ ← zero("root group")
        _ ← zero("reserved")
        _ ← zero("shared header msg format")
        _ ← expect("offset size", 8 toByte)
        _ ← expect("length size", 8 toByte)
        _ ← zero("reserved")
        groupLeafK ← unsignedShort()
        groupInternalK ← unsignedShort()
        _ ← expect("file consistency flags", 0, 4)
        base ← offset("base address")
        _ ← undefined("file free space info")
        eof ← offset("end of file")
        _ ← undefined("driver information")
        root ← SymbolTable.Entry[F]()
      } yield
        V0(
          base,
          eof,
          root
        )
    }
  }

  private val magic: Array[Byte] = Array[Byte](0x89.toByte, 'H', 'D', 'F', '\r', '\n', 0x1a, '\n')
  private val MAX_HEADER_SIZE = 64

  sealed trait ParseException extends Exception

  case object WrongMagicBytes extends ParseException

  case class SuperblockMagicNotFound(uri: URI) extends ParseException

  def apply(file: Uri)(implicit ec: ExecutionContext): Future[Header] =
    for {
      b ← Buffer(file)
      header ← apply()(MonadErr[Future], b)
    } yield
      header

  def apply[F[+_]: MonadErr]()(implicit b: Buffer[F]): F[Header] = {
    val s = syntax(b); import s._

    def superblock(offset: Long = 0): F[Long] = {
      for {
        _ ← b.seek(offset)
        _ ← expect("magic", magic)
      } yield
        offset
    }
    .recoverWith {
      case e: EOFException ⇒ e.raiseError[F, Long]
      case e ⇒ superblock {
        if (offset == 0) 512
        else 2 * offset
      }
    }

    for {
      offset ← superblock()
      _ ← zero("superblock")
      superblock ← V0[F]
    } yield
      superblock
  }
}
