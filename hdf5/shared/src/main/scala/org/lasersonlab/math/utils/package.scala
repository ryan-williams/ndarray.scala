package org.lasersonlab.math

package object utils {
  def roundUp(n: Int, base: Int): Int =
    n + (base - (n % base)) % base
}
