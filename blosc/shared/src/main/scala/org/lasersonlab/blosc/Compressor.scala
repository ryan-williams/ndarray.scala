package org.lasersonlab.blosc

sealed trait Compressor {}

object Compressor {
  case object LZ extends Compressor
  case object LZ4 extends Compressor
  case object LZ4HC extends Compressor
  case object Snappy extends Compressor
  case object ZLib extends Compressor
  case object ZStd extends Compressor
}
