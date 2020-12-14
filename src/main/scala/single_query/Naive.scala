package single_query

import utils.Utils.Query
import utils.Helpers.distance
import models.Data_naive

import scala.collection.mutable.ListBuffer

class Naive(c_query: Query) {

  @transient private var counter: Int = _
  @transient private var cpu_time: Long = 0L

  val slide: Int = c_query.S
  val R: Double = c_query.R
  val k: Int = c_query.k

  def process(elements: ListBuffer[(Int, Data_naive)]):Int = {

    //Metrics
    counter += 1
    val time_init = System.currentTimeMillis()

    val inputList = elements.map(_._2).toList

    var outliers = 0
    inputList.foreach(p => {
      if (!p.safe_inlier) {
        outliers += 1
      }
    })

    //Metrics
    val time_final = System.currentTimeMillis()
    cpu_time += (time_final - time_init)
    outliers
  }

  def refreshList(node: Data_naive, nodes: List[Data_naive]): Unit = {
    if (nodes.nonEmpty) {
      val neighbors = nodes
        .filter(_.id != node.id)
        .map(x => (x, distance(x.value.toArray, node.value.toArray)))
        .filter(_._2 <= R).map(_._1)

      neighbors
        .foreach(x => {
            node.count_after += 1
            if (node.count_after >= k) {
              node.safe_inlier = true
            }
        })

      nodes
        .filter(x => neighbors.contains(x))
        .foreach(n => {
          n.count_after += 1
          if (n.count_after >= k) {
            n.safe_inlier = true
          }
        }) //add new neighbor to previous nodes
    }
  }

}