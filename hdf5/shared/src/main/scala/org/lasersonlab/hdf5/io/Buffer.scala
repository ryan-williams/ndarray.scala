package org.lasersonlab.hdf5.io

import java.nio.{ ByteBuffer, ByteOrder }
import ByteOrder.LITTLE_ENDIAN
import ByteBuffer.wrap

import cats.implicits._
import cats.{ Monad, MonadError }
import hammerlab.collection._
import hammerlab.either._
import hammerlab.option._
import org.lasersonlab.files.Uri
import org.lasersonlab.hdf5.io.Buffer.{ EOFException, MonadErr }
import org.lasersonlab.hdf5.{ Addr, Length }

import scala.Array.fill
import scala.concurrent.{ ExecutionContext, Future }
import scala.math.min

case class Buffer[F[+_]: MonadErr](fetch: Long ⇒ F[ByteBuffer]) {

  private val F = MonadErr[F]
  import F.rethrow

  private var range: F[(Long, ByteBuffer, Long)] = range(0)

  private def start: F[Long] = range.map(_._1)
  private def   end: F[Long] = range.map(_._3)
  private def buf: F[ByteBuffer] = range.map(_._2)

  private def range(pos: Long): F[(Long, ByteBuffer, Long)] =
    fetch(pos).map {
      b ⇒
        (
          pos - b.position(),
          b,
          pos + b.remaining()
        )
    }

  private def advance(): F[Unit] =
    for {
      end ← end
      _ ← range(end)
    } yield
      ()

  def slice(start: Addr, length: Length): Buffer[F] =
    Buffer {
      pos ⇒
        val end = start + length

        rethrow {
          if (pos >= length)
            L(EOFException(start, end, pos)).pure[F]
          else
            fetch(start + pos).map {
              b ⇒
                val dupe = b.duplicate().order(b.order)  // JDK-4715166 😱
                if (pos + dupe.remaining() > length)
                  dupe.limit((length - pos) toInt)

                R(dupe)
            }
        }
    }

  def seek(pos: Long): F[Unit] =
    for {
      t ← range
      (start, b, end) = t
      _ ←
        if (start <= pos && pos < end) {
          b.position((pos - start) toInt)
          (()).pure[F]
        } else
          range(pos)
    } yield
      ()

  def consume[T](length: Length)(fn: Buffer[F] ⇒ F[T]): F[T] = {
    for {
      pos ← position
      sliced = slice(pos, length)
      t ← fn(sliced)
      _ ← seek(pos + length)
    } yield
      t
  }

  private def get[T](bytes: Int, fn: ByteBuffer ⇒ T): F[T] =
    buf >>= {
      b ⇒
        if (b.remaining() >= bytes)
          fn(b).pure[F]
        else
          get(bytes, fn, fill(bytes)(0 toByte), order = b.order(), offset = 0)
    }

  private def get[T](
    bytes: Int,
    fn: ByteBuffer ⇒ T,
    arr: Array[Byte],
    order: ByteOrder,
    offset: Int
  ):
    F[T] =
    if (bytes == 0)
      fn(wrap(arr).order(order)).pure[F]
    else
      for {
        b ← buf
        remaining = b.remaining()
        res ←
          if (remaining == 0) {
            for {
              _ ← advance()
              res ← buf >>= { b ⇒ get(bytes, fn, arr, order, offset) }
            } yield
              res
          } else {
            val toRead = min(bytes, remaining)
            b.get(arr, offset, toRead)
            val left = bytes - toRead
            get(bytes - toRead, fn, arr, order, offset + toRead)
          }
      } yield
        res

  def get     : F[ Byte] = get(1, _.get)
  def get[T](fn: (Long, Byte) ⇒ T): F[T] = position.map2(get) { fn }

  def getShort: F[Short] = get(2, _.getShort)
  def getShort[T](fn: (Long, Short) ⇒ T): F[T] = position.map2(getShort) { fn }

  def getInt  : F[  Int] = get(4, _.getInt)
  def getInt[T](fn: (Long, Int) ⇒ T): F[T] = position.map2(getInt) { fn }

  def getLong : F[ Long] = get(8, _.getLong)
  def getLong[T](fn: (Long, Long) ⇒ T) : F[T] = position.map2(getLong) { fn }

  def get(arr: Array[Byte], order: ByteOrder = LITTLE_ENDIAN): F[Array[Byte]] =
    get(
      arr.length,
      _ ⇒ arr,
      arr,
      order,
      offset = 0
    )

  def burn(length: Int): F[Unit] = get(fill(length)(0 toByte)).map { _ ⇒ () }

  def get[T](arr: Array[Byte], fn: (Long, Array[Byte]) ⇒ T): F[T] =
    position.map2(get(arr)) { fn }

  def position: F[Long] = buf.map2(start) { _.position() + _ }
}

object Buffer {
  case class UnsupportedValue[T](name: String, value: T, pos: Long) extends Exception(s"$name: $value at pos $pos")

  def apply(file: Uri, littleEndian: Boolean = true)(implicit ec: ExecutionContext): Future[Buffer[Future]] =
    for {
      size ← file.size
    } yield
      Buffer[Future] {
        pos ⇒
          import file.blockSize
          val start = pos / blockSize * blockSize
          file.bytes(start, blockSize).map {
            bytes ⇒
              val buff = wrap(bytes)
              if (littleEndian)
                buff.order(ByteOrder.LITTLE_ENDIAN)
              else
                buff
          }
      }
      .slice(0, size)

  type MonadErr[F[_]] = MonadError[F, Throwable]
  object MonadErr {
    def apply[F[_]](implicit F: MonadErr[F]) = F
  }

  case class syntax[F[+_]](buf: Buffer[F])(implicit F: MonadErr[F]) {
    import F.{ rethrow, raiseError }

    def offset(name: String): F[  Addr] = unsignedLong(name)
    def length(name: String): F[Length] = unsignedLong(name)

    def offset_?(name: String): F[?[Addr]] =
      rethrow {
        buf.getLong {
          (position, long) ⇒
            if (long == -1)
              R(None)
            else if (long < 0)
              L(UnsupportedValue(name, long, position - 8))
            else
              R(Some(long))
        }
      }

    def int(name: String, expected: Int) =
      rethrow {
        buf.getInt[Exception | Unit] {
          (position, int) ⇒
            if (int != expected)
              L(UnsupportedValue(name, int, position))
            else
              R(())
        }
      }

    def expect(name: String, value: Byte): F[Unit] =
      rethrow {
        buf.get {
          (pos, byte) ⇒
            if (byte != value)
              L(UnsupportedValue(name, byte, pos))
            else
              R(())
        }
      }

    def expect(name: String, expected: Array[Byte]): F[Unit] = {
      val actual = fill(expected.length)(0 toByte)
      rethrow {
        buf.get(actual, {
          (pos, actual) ⇒
            if (!actual.sameElements(expected))
              L(UnsupportedValue(name, actual, pos))
            else
              R(())
        })
      }
    }

    def expect(name: String, value: Byte, num: Int): F[Unit] = {
      if (num == 0) ().pure[F]
      else
        for {
          _ ← expect(name, value)
          rest ← expect(name, value, num - 1)
        } yield
          rest
    }

    def zero(name: String) = expect(name, 0 toByte)

    def unsignedShort(): F[Int] =
      buf.getShort.map {
        short ⇒
          if (short < 0)
            short.toInt + 0xffff
          else
            short
      }

    def unsignedByte(): F[Short] =
      buf.get.map {
        byte ⇒
          if (byte < 0)
            (byte + 0xff).toShort
          else
            byte
      }

    def unsignedInt(): F[Long] =
      buf.getInt.map {
        int ⇒
          if (int < 0)
            int + 0xffffffffL
          else
            int
      }

    def unsignedLong(name: String): F[Long] =
      for {
        pos ← buf.position
        long ← buf.getLong
        res ← {
          if (long < 0)
            raiseError { UnsupportedValue(name, long, pos) }
          else
            long.pure[F]
        }
      } yield
        res

    def unsignedLongs(name: String): F[List[Long]] = unsignedLongs(name, buf.position)
    def unsignedLongs(name: String, pos: F[Long], longs: F[List[Long]] = List[Long]().pure[F]): F[List[Long]] =
      buf.getLong >>= {
        next ⇒
          if (next < 0)
            pos >>= {
              pos ⇒
                raiseError {
                  UnsupportedValue(name, next, pos)
                }
            }
          else
            longs.map {
              longs ⇒
                if (next == 0)
                  longs
                else
                  next :: longs
            }
      }

    def undefined(name: String) = expect(name, 0xff toByte, 8)

    def bytes(name: String, size: Int): Array[Byte] = {
      val arr = fill[Byte](size)(0)
      buf.get(arr)
      arr
    }
  }

  case class EOFException(start: Long, end: Long, pos: Long) extends Exception
}
