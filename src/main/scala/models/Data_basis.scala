package models

import java.time.ZoneOffset
import java.time.format.DateTimeFormatter
import java.time.temporal.{ChronoField, TemporalAccessor}

import org.apache.spark.sql.DataFrame
import outlier_detection.Outlier_detection.{dateTimeStringToEpoch, delimiter}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

import scala.collection.mutable.ListBuffer

object ExtraDataFrameOperations {
  object implicits {
    implicit def dFWithExtraOperations(df: List[String],flag: Int) = Data_basis(df, flag)
  }
}

case class Data_basis(row: List[String],c_flag: Int) extends Serializable {

  val dataframe: List[String] = row
  val id: Int = row(0).toString().split(",")(0).toInt
  val value: ListBuffer[Double] = row(0).toString().split(",")(1).split(delimiter).map(_.toDouble).to[ListBuffer]
  val dimensions: Int = value.length
  val arrival: Long = dateTimeStringToEpoch(row(1),"yyyy-MM-dd HH:mm:ss.SSS")
  val flag: Int = c_flag
  val state: Seq[ListBuffer[Double]] = Seq(value)
  val hashcode: Int = state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)

  def this(dfPoint: Data_basis){
    this(dfPoint.dataframe,dfPoint.flag)
  }


  def compareTo(t: Data_basis): Int = {
    val dim = Math.min(this.dimensions, t.dimensions)
    for (i <- 0 until dim) {
      if (this.value(i) > t.value(i)) +1
      else if (this.value(i) < t.value(i)) -1
      else 0
    }
    if (this.dimensions > dim) +1
    else -1
  }

  override def toString = s"Data_basis($id, $value, $arrival, $flag)"

}