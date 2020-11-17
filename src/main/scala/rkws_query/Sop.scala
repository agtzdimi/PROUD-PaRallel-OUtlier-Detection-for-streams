package rkws_query

import utils.Helpers.distance
import utils.Utils.Query
import models.{Data_lsky, Data_mcod}
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

case class SopState(var index: mutable.LinkedHashMap[Int, Data_lsky], var slide_count: Long)

class Sop(c_queries: ListBuffer[Query], c_slide: Int) {

  @transient private var counter: Int = _
  @transient private var cpu_time: Long = 0L

  val queries: ListBuffer[Query] = c_queries
  val slide: Int = c_slide
  val R_distinct_list = queries.map(_.R).distinct.sorted
  val k_distinct_list = queries.map(_.k).distinct.sorted
  val W_distinct_list = queries.map(_.W).distinct.sorted
  val S_distinct_list = queries.map(_.S).distinct.sorted
  val R_max = R_distinct_list.max
  val R_min = R_distinct_list.min
  val k_max = k_distinct_list.max
  val k_min = k_distinct_list.min
  val k_size = k_distinct_list.size
  val R_size = R_distinct_list.size
  val W_size = W_distinct_list.size
  val S_size = S_distinct_list.size
  val S_report_times = S_distinct_list.map(p => p / slide).sorted
  val S_max_report = S_report_times.max

  def process(elements: Dataset[(Int, Data_lsky)], windowEnd: Long, spark: SparkSession, windowStart: Long): Query = {

    //Metrics
    counter += 1
    val time_init = System.currentTimeMillis()

    val inputList = elements.rdd.collect().toList.toIterable
    //Create state
    val index = mutable.LinkedHashMap[Int, Data_lsky]()
    val cur_slide: Long =  windowEnd / slide
    val current = SopState(index, cur_slide)

    var output_slide = ListBuffer[Int]()
    S_report_times.foreach(p => {
      if (current.slide_count % p == 0) output_slide += p
    })

    val all_queries = Array.ofDim[Int](R_size, k_size, W_size)

    //Insert new elements to state
    inputList
      .filter(_._2.arrival >= windowEnd - slide)
      //Sort is needed when each point has a different timestamp
      //In our case every point in the same slide has the same timestamp
      .toList
      .sortBy(_._2.arrival)
      .foreach(p => current.index += ((p._2.id, p._2)))

    //Update inputList
    inputList
      .foreach(p => {
        if (!p._2.safe_inlier && p._2.flag == 0) {
          checkPoint(p._2, windowEnd, current, windowStart)
          if (p._2.lSky.getOrElse(1, ListBuffer()).count(_._2 >= p._2.arrival) >= k_max) p._2.safe_inlier = true
          else {
            if (output_slide.nonEmpty) {
              var w: Int = 0
              do {
                if (p._2.arrival >= windowEnd - W_distinct_list(w)) {
                  var i, y: Int = 0
                  var count = p._2.lSky.getOrElse(i + 1, ListBuffer()).count(_._2 >= windowEnd - W_distinct_list(w))
                  do {
                    if (count >= k_distinct_list(y)) { //inlier for all i
                      y += 1
                    } else { //outlier for all y
                      for (z <- y until k_size) {
                        all_queries(i)(z)(w) += 1
                      }
                      i += 1
                      count += p._2.lSky.getOrElse(i + 1, ListBuffer()).count(_._2 >= windowEnd - W_distinct_list(w))
                    }
                  } while (i < R_size && y < k_size)
                }
                w += 1
              } while (w < W_size)
            }
          }
        }
      })

    val slide_to_report = output_slide.map(_ * slide)
    var tmpQuery = Query(R_distinct_list(0), k_distinct_list(0), W_distinct_list(0), slide_to_report.head, all_queries(0)(0)(0))
    if (output_slide.nonEmpty) {
      val slide_to_report = output_slide.map(_ * slide)
      for (i <- 0 until R_size) {
        for (y <- 0 until k_size) {
          for (z <- 0 until W_size) {
            slide_to_report.foreach(p => {
              tmpQuery = Query(R_distinct_list(i),k_distinct_list(y),W_distinct_list(z), p,all_queries(i)(y)(z))
            })
          }
        }
      }
    }
    current.slide_count += 1

    //Remove old points
    inputList
      .filter(p => p._2.arrival < windowStart + slide)
      .foreach(p => {
        deletePoint(p._2, current)
      })

    //Metrics
    val time_final = System.currentTimeMillis()
    cpu_time += (time_final - time_init)
    tmpQuery
  }

  def checkPoint(el: Data_lsky, windowEnd: Long, current: SopState, windowStart: Long): Unit = {
    if (el.lSky.isEmpty) { //It's a new point!
      insertPoint(el, current)
    } else { //It's an old point
      updatePoint(el, windowEnd, current, windowStart)
    }
  }

  def insertPoint(el: Data_lsky, current: SopState): Unit = {
    current.index.values.toList.reverse //get the points so far from latest to earliest
      .takeWhile(p => {
        var res = true //variable to stop skyband loop
        if (p.id != el.id) {
          val thisDistance = distance(el.value.toArray, p.value.toArray)
          if (thisDistance <= R_max) {
            val skyRes = neighborSkyband(el, p, thisDistance)
            if (!skyRes && thisDistance <= R_min) {
              res = false
            }
          }
        }
        res
      })
  }

  def updatePoint(el: Data_lsky, windowEnd: Long, current: SopState, windowStart: Long): Unit = {
    //Remove old points from lSky
    el.lSky.keySet.foreach(p => el.lSky.update(p, el.lSky(p).filter(_._2 >= windowStart)))
    //Create input
    val old_sky = el.lSky.values.flatten.toList.sortWith((p1, p2) => p1._2 > p2._2).map(_._1)
    el.lSky.clear()

    var res = true //variable to stop skyband loop
    current.index.values.toList.reverse
      .takeWhile(p => {
        var tmpRes = true
        if (p.arrival >= windowEnd - slide) {
          val thisDistance = distance(el.value.toArray, p.value.toArray)
          if (thisDistance <= R_max) {
            val skyRes = neighborSkyband(el, p, thisDistance)
            if (!skyRes && thisDistance <= R_min) res = false
          }
        } else tmpRes = false
        res && tmpRes
      })
    if (res)
      old_sky
        .takeWhile(l => { //Time to check the old skyband inputList
          val p = current.index(l)
          val thisDistance = distance(el.value.toArray, p.value.toArray)
          if (thisDistance <= R_max) {
            val skyRes = neighborSkyband(el, p, thisDistance)
            if (!skyRes && thisDistance < R_min) res = false
          }
          res
        })
  }

  def deletePoint(el: Data_lsky, current: SopState): Unit = {
    current.index.remove(el.id)
  }

  def neighborSkyband(el: Data_lsky, neigh: Data_lsky, distance: Double): Boolean = {
    val norm_dist = normalizeDistance(distance)
    var count = 0
    for (i <- 1 to norm_dist) {
      count += el.lSky.getOrElse(i, ListBuffer[Long]()).size
    }
    if (count <= k_max - 1) {
      el.lSky.update(norm_dist, el.lSky.getOrElse(norm_dist, ListBuffer[(Int, Long)]()) += ((neigh.id, neigh.arrival)))
      true
    } else {
      false
    }
  }

  def normalizeDistance(distance: Double): Int = {
    var res, i = 0
    do {
      if (distance <= R_distinct_list(i)) res = i + 1
      i += 1
    } while (i < R_distinct_list.size && res == 0)
    res
  }

}
