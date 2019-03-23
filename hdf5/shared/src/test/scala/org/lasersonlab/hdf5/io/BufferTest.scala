package org.lasersonlab.hdf5.io

import java.nio.ByteBuffer

import cats.implicits._
import org.lasersonlab.cmp.Assert
import utest._

import scala.concurrent.Future

object BufferTest
  extends lasersonlab.Suite
    with Assert.syntax {
  val tests = Tests {
    val file = resource("numbers")

    'write - {
      if (false) {
        file.write(
          (
            for {
              n ← 1 to 10
            } yield
              1 to n mkString
            )
            .mkString("\n")
        )
      } else
        Future()
    }

    'simple - {
      import Array.fill
      for {
        b ← Buffer(file)
        bytes ← b.get(fill(10)(0 toByte))
        str = new String(bytes, "UTF-8")
        _ ← ==(str, "1\n12\n123\n1")
      } yield
        ()
    }
  }
}
