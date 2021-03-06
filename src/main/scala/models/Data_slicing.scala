package models

import mtree.DistanceFunctions.EuclideanCoordinate

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object ExtraDataFrameOperationsSlicing {
  object implicits {
    implicit def dFWithExtraOperations(c_id: Int, c_val: ListBuffer[Double], c_arrival: Long, c_flag: Int) = Data_basis(c_id: Int, c_val: ListBuffer[Double], c_arrival: Long, c_flag: Int)
  }
}

case class Data_slicing(c_point: Data_basis) extends Serializable with EuclideanCoordinate with Comparable[Data_slicing] with Ordered[Data_slicing]  {

  //Micro-cluster data
  var mc: Int = -1
  var Rmc = mutable.HashSet[Int]()
  val id: Int = c_point.c_id
  val value: ListBuffer[Double] = c_point.c_val
  val dimensions: Int = value.length
  var arrival: Long = c_point.c_arrival
  val flag: Int = c_point.c_flag
  val state: Seq[ListBuffer[Double]] = Seq(value)
  val hashcode: Int = state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  var nn_before = ListBuffer[Long]()
  //Neighbor data
  var count_after: Int = 0
  var slices_before: mutable.HashMap[Long, Int] = mutable.HashMap[Long, Int]()
  //Skip flag
  var safe_inlier: Boolean = false
  //Slice check
  var last_check: Long = 0L

  override def canEqual(other: Any): Boolean = other.isInstanceOf[Data_slicing]

  override def equals(other: Any): Boolean = other match {
    case that: Data_slicing =>
      this.value.size == that.value.size &&
        this.value == that.value &&
        this.id == that.id
    case _ => false
  }

  override def hashCode(): Int = {
    hashcode
  }

  /**
    * A method to access the {@code index}-th component of the coordinate.
    *
    * @param index The index of the component. Must be less than { @link
    *              #dimensions()}.
    */
  override def get(index: Int): Double = value(index)

  override def compareTo(t: Data_slicing): Int = {
    val dim = Math.min(this.dimensions, t.dimensions)
    for (i <- 0 until dim) {
      if (this.value(i) > t.value(i)) +1
      else if (this.value(i) < t.value(i)) -1
      else 0
    }
    if (this.dimensions > dim) +1
    else -1
  }

  override def compare(that: Data_slicing): Int = this.compareTo(that)
}