package org.lasersonlab.hdf5

import java.net.URI
import java.nio.ByteBuffer

import hammerlab.either._
import org.lasersonlab.files.Uri
import org.lasersonlab.hdf5.io.Buffer

import scala.concurrent.{ ExecutionContext, Future }

sealed trait Header {
  def base: Long
}

object Header {

  case class V0(
    base: Long,
     eof: Long,
    root: SymbolTable
  )
  extends Header
  object V0 {
    def apply(implicit buffer: Buffer): Exception | V0 = {
      import buffer._

      for {
        _ ← expect("magic", magic)
        _ ← zero("superblock")
        _ ← zero("free space")
        _ ← zero("root group")
        _ ← zero("reserved")
        _ ← zero("shared header msg format")
        _ ← expect("offset size", 8 toByte)
        _ ← expect("length size", 8 toByte)
        _ ← zero("reserved")
        groupLeafK = unsignedShort()
        groupInternalK = unsignedShort()
        _ ← expect("file consistency flags", 0, 4)
        base ← offset("base address")
        _ ← undefined("file free space info")
        eof ← offset("end of file")
        _ ← undefined("driver information")
        root ← SymbolTable()
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

  def apply(file: Uri)(implicit ec: ExecutionContext): Unit = {
    file
      .size
      .map {
        size ⇒
          lazy val offsets: Stream[Long] = 512L #:: offsets.map{ _ * 2 }

          def superblock(offset: Long = 0): Future[Long] =
            if (offset + magic.length >= size)
              Future.failed {
                SuperblockMagicNotFound(file.uri)
              }
            else
              file
                .bytes(offset, magic.length)
                .flatMap {
                  case bytes if bytes.sameElements(magic) ⇒ Future { offset }
                  case _ ⇒
                    superblock {
                      if (offset == 0)
                        512
                      else
                        offset * 2
                    }
                }

          for {
            offset ← superblock()
            bytes ← file.bytes(offset, MAX_HEADER_SIZE)
            buffer = ByteBuffer.wrap(bytes, magic.length, MAX_HEADER_SIZE - magic.length)
          } {

          }

      }
  }
}
