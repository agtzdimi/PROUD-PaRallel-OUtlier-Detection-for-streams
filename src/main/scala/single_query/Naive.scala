package single_query

import models.Data_naive
import org.apache.spark.sql.{Dataset, SparkSession}
import utils.Helpers.distance
import utils.Utils.Query

import scala.collection.mutable.ListBuffer

class Naive(c_query: Query) {

  val slide: Int = c_query.S
  val R: Double = c_query.R
  val k: Int = c_query.k
  @transient private var counter: Int = _
  @transient private var cpu_time: Long = 0L

  def process(elements: Dataset[(Int, Data_naive)], windowEnd: Long, spark: SparkSession):scala.Iterable[Data_naive] = {
    //Metrics
    counter += 1
    val time_init = System.currentTimeMillis()
    val inputList = elements.rdd.map(_._2).collect().toList

    inputList.filter(_.arrival >= windowEnd - slide).foreach(p => {
      refreshList(p, inputList, windowEnd)
    })
    var iter = ListBuffer[Data_naive]();

    inputList.foreach(p => {
      println(p)
      if (!p.safe_inlier) {
        iter += p
        print(p)
      }
    })


    //Metrics
    val time_final = System.currentTimeMillis()
    cpu_time += (time_final - time_init)
    return iter.toIterable
  }

  def refreshList(node: Data_naive, nodes: List[Data_naive], windowEnd: Long): Unit = {
    if (nodes.nonEmpty) {
      val neighbors = nodes
        .filter(_.id != node.id)
        .map(x => (x, distance(x.value.toArray, node.value.toArray)))
        .filter(_._2 <= R).map(_._1)

      neighbors
        .foreach(x => {
          if (x.arrival < windowEnd - slide) {
            node.insert_nn_before(x.arrival, k)
          } else {
            node.count_after += 1
            if (node.count_after >= k) {
              node.safe_inlier = true
            }
          }
        })

      nodes
        .filter(x => x.arrival < windowEnd - slide && neighbors.contains(x))
        .foreach(n => {
          n.count_after += 1
          if (n.count_after >= k) {
            n.safe_inlier = true
          }
        }) //add new neighbor to previous nodes
    }
  }

}