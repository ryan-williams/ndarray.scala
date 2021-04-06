package org.lasersonlab.blosc

sealed trait Shuffle

object Shuffle {
  case object None extends Shuffle
  case object Bytes extends Shuffle
  case object Bits extends Shuffle
}
