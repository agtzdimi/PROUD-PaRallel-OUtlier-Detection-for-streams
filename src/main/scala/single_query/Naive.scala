package single_query

import utils.Utils.Query
import utils.Helpers.distance
import models.Data_naive
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ListBuffer

class Naive(c_query: Query) {

  @transient private var cpu_time: Long = 0L

  val slide: Int = c_query.S
  val R: Double = c_query.R
  val k: Int = c_query.k

  def process(element: (Int, Data_naive), data: DataFrame, spark: SparkSession) = {

    import spark.implicits._

    var outliers = new ListBuffer[(Int, Data_naive)]()
    //Metrics
    val time_init = System.currentTimeMillis()

    val inputList = data.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)").map(r => (r.getAs[Int](0), r.getAs[Data_naive](1))).collect.toList

    inputList.foreach(p => {
      refreshList(p, inputList)
    })
    inputList.foreach(p => {
      if (!p._2.safe_inlier) {
        println(p)
        outliers += p
      }
    })

    //Metrics
    val time_final = System.currentTimeMillis()
    cpu_time += (time_final - time_init)

    outliers
  }

  def refreshList(node: (Int, Data_naive), nodes: List[(Int, Data_naive)]): Unit = {
    if (nodes.nonEmpty) {
      val neighbors = nodes
        .filter(_._2.id != node._2.id)
        .map(x => (x, distance(x._2.value.toArray, node._2.value.toArray)))
        .filter(_._2 <= R).map(_._1)

      neighbors
        .foreach(x => {
          node._2.count_after += 1
            if (node._2.count_after >= k) {
              node._2.safe_inlier = true
            }
        })

      nodes
        .filter(x => neighbors.contains(x))
        .foreach(n => {
          n._2.count_after += 1
          if (n._2.count_after >= k) {
            n._2.safe_inlier = true
          }
        }) //add new neighbor to previous nodes
    }
  }

}