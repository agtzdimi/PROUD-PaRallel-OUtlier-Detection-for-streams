package models

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object ExtraDataFrameOperationsMcod {
  object implicits {
    implicit def dFWithExtraOperations(c_id: Int, c_val: ListBuffer[Double], c_arrival: Long, c_flag: Int) = Data_basis(c_id: Int, c_val: ListBuffer[Double], c_arrival: Long, c_flag: Int)
  }
}

case class Data_mcod(c_point: Data_basis) extends Serializable {

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
  var count_after: Int = 0
  var nn_before = ListBuffer[Long]()
  //Skip flag
  var safe_inlier: Boolean = false

  //Clear variables
  def clear(newMc: Int): Unit = {
    nn_before.clear()
    count_after = 0
    mc = newMc
  }

  //Function to insert data as a preceding neighbor (max k neighbors)
  def insert_nn_before(el: Long, k: Int = 0): Unit = {
    if (k != 0 && nn_before.size == k) {
      val tmp = nn_before.min
      if (el > tmp) {
        nn_before.-=(tmp)
        nn_before.+=(el)
      }
    } else {
      nn_before.+=(el)
    }
  }

  //Get the minimum of preceding neighbors
  def get_min_nn_before(time: Long): Long = {
    if (nn_before.count(_ >= time) == 0) 0L
    else nn_before.filter(_ >= time).min
  }

  def compareTo(t: Data_naive): Int = {
    val dim = Math.min(this.dimensions, t.dimensions)
    for (i <- 0 until dim) {
      if (this.value(i) > t.value(i)) +1
      else if (this.value(i) < t.value(i)) -1
      else 0
    }
    if (this.dimensions > dim) +1
    else -1
  }

  override def toString = s"Data_mcod($id, $value, $arrival, $flag)"

}
