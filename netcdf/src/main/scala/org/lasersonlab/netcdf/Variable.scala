package org.lasersonlab.netcdf

import hammerlab.lines._
import hammerlab.show._
import org.lasersonlab.netcdf.show._
import ucar.ma2.DataType

import scala.collection.JavaConverters._

case class Variable(
  name: String,
  description: Option[String],
  dtype: DataType,  // TODO: replace with our own DataType, for cross-compilation
  attrs: Seq[Attribute],
  dimensions: Seq[Dimension],
  rank: Int,
  shape: Seq[Int],
  size: Long,
  data: ucar.nc2.Variable
)

object Variable {
  implicit def apply(v: ucar.nc2.Variable): Variable =
    Variable(
      v.getShortName,
      Option(v.getDescription),
      v.getDataType,
      v.getAttributes.asScala.map { Attribute(_) },
      v.getDimensions.asScala.map { Dimension(_) },
      v.getRank,
      v.getShape,
      v.getSize,
      v
    )

  implicit def lines: ToLines[Variable] =
    ToLines {
      case Variable(
        name,
        description,
        dtype,
        attrs,
        dimensions,
        rank,
        _,
        size,
        _
      ) ⇒
        val descriptionString =
          description
          .filter(_.nonEmpty)
          .fold("") {
            d ⇒ show" ($d)"
          }
        Lines(
          show"$name:$descriptionString ($dtype, $size)",
          indent(
            show"dimensions ($rank): $dimensions",
            attrs
          )
        )
    }
}

