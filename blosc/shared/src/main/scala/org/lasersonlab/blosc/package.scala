package org.lasersonlab

import hammerlab.math._
import java.nio.ByteBuffer

package object blosc {
  val VERSION_FORMAT = 2
  val MIN_HEADER_LENGTH = 16
  val MAX_OVERHEAD = MIN_HEADER_LENGTH
  val MAX_BUFFERSIZE = Int.MaxValue - MIN_HEADER_LENGTH
  val MAX_TYPESIZE = 255

  def compress(
    clevel: Int,
    doshuffle: Int,
    _typesize: Int,
    sourcesize: Int,
    src: ByteBuffer,
    dest: ByteBuffer,
    destsize: Int,
    compressor: Compressor
  ) = {
    val typesize = if (_typesize > MAX_TYPESIZE) 1 else _typesize
    ///val blocksize =
//    val (nblocks, leftover) =
//      if (sourcesize < typesize)
//        (1, sourcesize)
//
//        sourcesize /↑ blocksize
//    val leftover = sourcesize % blocksize
  }

  def decompress(
    src: ByteBuffer,
    dest: ByteBuffer,
    destsize: Int
  ) = {}
}
