package org.lasersonlab.hdf5.io

import java.nio.{ ByteBuffer, ByteOrder }
import ByteOrder.LITTLE_ENDIAN
import ByteBuffer.wrap
import java.nio.charset.Charset

import cats.implicits._
import cats.{ Monad, MonadError }
import hammerlab.collection._
import hammerlab.either._
import hammerlab.option._
import org.lasersonlab.files.Uri
import org.lasersonlab.hdf5.io.Buffer.{ EOFException, MonadErr }
import org.lasersonlab.hdf5.{ Addr, Length }

import scala.Array.fill
import scala.collection.mutable
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

  def takeBytes[T](length: Length)(fn: Buffer[F] ⇒ F[T]): F[Vector[T]] =
    consume(length) {
      _.take(fn)
    }

  def takeN[T](num: Int)(fn: Buffer[F] ⇒ F[T]): F[Vector[T]] = takeN(fn, num, Vector())
  def takeN[T](fn: Buffer[F] ⇒ F[T], num: Int, elems: Vector[T]): F[Vector[T]] =
    if (num == 0)
      elems.pure[F]
    else
      fn(this).>>= {
        elem ⇒ takeN(fn, num - 1, elems :+ elem)
      }

  def take[T](fn: Buffer[F] ⇒ F[T]): F[Vector[T]] = take(fn, Vector())
  private def take[T](fn: Buffer[F] ⇒ F[T], elems: Vector[T]): F[Vector[T]] =
    fn(this).>>= {
      elem ⇒
        take(fn, elems :+ elem)
    }
    .recover {
      case e: EOFException ⇒ elems
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

  def getN[T](n: Int, arr: Array[Byte], fn: ByteBuffer ⇒ T, order: ByteOrder = LITTLE_ENDIAN): F[T] =
    get(n, _ ⇒ arr, arr, order, offset = 0).map {
      arr ⇒
        fn(wrap(arr).order(order))
    }

  def burn(length: Int): F[Unit] = get(fill(length)(0 toByte)).map { _ ⇒ () }

  def get[T](arr: Array[Byte], fn: (Long, Array[Byte]) ⇒ T): F[T] =
    position.map2(get(arr)) { fn }

  def position: F[Long] = buf.map2(start) { _.position() + _ }

  private def bytesUntilNull(bytes: mutable.ArrayBuilder[Byte]): F[Array[Byte]] =
    buf.>>= {
      buf ⇒
        var byte: Byte = 0
        while (buf.remaining() > 0 && ({ byte = buf.get(); byte } != 0)) {
          bytes += byte
        }
        if (byte == 0)
          bytes.result().pure[F]
        else
          advance() >>= {
            _ ⇒ bytesUntilNull(bytes)
          }
    }

  def ascii: F[String] =
    bytesUntilNull(Array.newBuilder[Byte])
      .map {
        bytes ⇒
          val cs = Charset.forName("ASCII")
          cs.decode(wrap(bytes)).toString
      }
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
    import F.rethrow

    def offset(name: String): F[  Addr] = unsignedLong(name)
    def length(name: String): F[Length] = unsignedLong(name)

    def offset_?(name: String): F[?[Addr]] =
      buf.getLong.>>= {
        long ⇒
          if (long < 0)
            err[Long, ?[Long]](name, long, 8)
          else {
            (
              if (long == -1)
                None
              else
                Some(long)
            )
            .pure[F]
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

    def byte(): F[Byte] = buf.get

    def err[V, T](name: String, value: V, offset: Int = 0): F[T] =
      buf.position.>>= {
        pos ⇒
          UnsupportedValue(name, value, pos - offset)
            .raiseError[F, T]
      }

    def bool(name: String): F[Boolean] =
      buf.get.>>= {
        case 0 ⇒ false.pure[F]
        case 1 ⇒  true.pure[F]
        case n ⇒ err(name, n, 1)
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

    def signedInt(name: String): F[Int] =
      buf.getInt >>= {
        int ⇒
          if (int < 0)
            err(name, int, 4)
          else
            int.pure[F]
      }

    def unsignedLong(name: String): F[Long] =
      for {
        long ← buf.getLong
        res ←
          if (long < 0)
            err(name, long, 8)
          else
            long.pure[F]
      } yield
        res

    def unsignedLongs(name: String, num: Int): F[Vector[Long]] = unsignedLongs(name, num, Vector())
    private def unsignedLongs(name: String, num: Int, longs: Vector[Long]): F[Vector[Long]] =
      if (num == 0)
        longs.pure[F]
      else
        for {
          long ← unsignedLong(name)
          longs ← unsignedLongs(name, num - 1, longs :+ long)
        } yield
          longs

    def unsignedLongs(name: String): F[Vector[Long]] = unsignedLongs(name, buf.position)
    def unsignedLongs(name: String, pos: F[Long], longs: Vector[Long] = Vector[Long]()): F[Vector[Long]] =
      for {
        long ← unsignedLong(name)
        longs ←
          if (long == 0)
            longs.pure[F]
          else
            unsignedLongs(name, pos, longs :+ long)
      } yield
        longs

    def undefined(name: String) = expect(name, 0xff toByte, 8)

    def bytes(name: String, size: Int): F[Array[Byte]] =
      buf.get(fill(size)(0 toByte))
  }

  case class EOFException(start: Long, end: Long, pos: Long) extends Exception
}
