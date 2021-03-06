package org.lasersonlab.zarr.cmp.untyped

import hammerlab.either._
import hammerlab.option._
import org.hammerlab.test.Cmp
import org.lasersonlab.zarr.FillValue.NonNull
import org.lasersonlab.zarr.array.metadata.untyped.Shaped
import org.lasersonlab.zarr.dtype._
import org.lasersonlab.zarr.dtype.DataType._
import org.lasersonlab.zarr.{ Dimension, FillValue, Metadata }
import shapeless.the

object metadata {
  def cmpT[T](implicit t: Cmp[T]): Cmp.Aux[T, (T, T)] =
    new Cmp[T] {
      type Diff = (T, T)
      def cmp(l: T, r: T): Option[Diff] = t(l, r).map(_ ⇒ (l, r))
    }

  def cmpFromDatatype[T](d: DataType.Aux[T]): Cmp.Aux[T, (T, T)] =
    (
      d match {
        case d @ DataType.untyped.Struct(_) ⇒ cmpT[untyped.Struct]
        case d @ DataType.  byte    ⇒ cmpT[  Byte]
        case d @ DataType. short(_) ⇒ cmpT[ Short]
        case d @ DataType.   int(_) ⇒ cmpT[   Int]
        case d @ DataType.  long(_) ⇒ cmpT[  Long]
        case d @ DataType. float(_) ⇒ cmpT[ Float]
        case d @ DataType.double(_) ⇒ cmpT[Double]
        case d @ DataType.string(_) ⇒ cmpT[String]
        case d @ DataType.Struct(_) ⇒
          new Cmp[d.T] {
            type Diff = (d.T, d.T)
            def cmp(l: d.T, r: d.T): Option[(d.T, d.T)] = (l != r) ? (l, r)
          }
      }
    )
    .asInstanceOf[
      Cmp.Aux[
        d.T,
        (
          d.T,
          d.T
        )
      ]
    ]

  implicit def fillValueCanEq[T](
    implicit
    d: DataType.Aux[T]
  ):
    Cmp.Aux[
      FillValue[T],
      d.T |
      d.T |
      (
        d.T,
        d.T
      )
    ] =
    Cmp {
      case (NonNull(l), NonNull(r)) ⇒ cmpFromDatatype(d)(l, r).map(R(_))
      case (NonNull(l), r) ⇒ Some(L(L(l)))
      case (l, NonNull(r)) ⇒ Some(L(R(r)))
      case _ ⇒ None
    }

  object base {
    def cmp[
      Shape[_],
      Idx
    ](
      implicit
      dim: Cmp[Shape[Dimension[Idx]]]
    ):
      Cmp[
        Shaped[
          Shape,
          Idx
        ]
      ] = {
      Cmp[
        Shaped[
          Shape,
          Idx
        ],
        Any  // TODO: fill in actual error type
      ] {
        (l, r) ⇒
          type T = r.T
          (l, r) match {
            case (
              l: Metadata[Shape, Idx, T],
              r: Metadata[Shape, Idx, T]
            ) ⇒
              implicit val d = l.dtype
              the[Cmp[Metadata[Shape, Idx, T]]].apply(l, r)
            case _ ⇒
              Some(s"Differing elem types: $l $r")
          }
      }
    }
  }
  trait cmp {
    implicit def baseCmp[
      Shape[_],
      Idx
    ](
      implicit
      dim: Cmp[Shape[Dimension[Idx]]]
    ):
      Cmp[
        Shaped[
          Shape,
          Idx
        ]
      ] =
      base.cmp
  }
  object cmp extends cmp
}
