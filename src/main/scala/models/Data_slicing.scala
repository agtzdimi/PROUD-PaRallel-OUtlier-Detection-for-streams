package models

import mtree.DistanceFunctions.EuclideanCoordinate

import scala.collection.mutable

class Data_slicing(c_point: Data_basis) extends Data_basis(c_point.dataframe, c_point.flag) with EuclideanCoordinate with Comparable[Data_slicing] with Ordered[Data_slicing]  {

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
