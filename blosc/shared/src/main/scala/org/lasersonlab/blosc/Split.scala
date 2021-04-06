package org.lasersonlab.blosc

sealed trait Split

object Split {
  case object Always extends Split
  case object Never extends Split
  case object Auto extends Split
  case object ForwardCompat extends Split
}
