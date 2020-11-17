package rk_query

import utils.Helpers.distance
import utils.Utils.Query
import models.{Data_amcod, Data_mcsky}
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

case class PmcskyCluster(var center: ListBuffer[Double], var points: ListBuffer[Int])
case class PmcskyState(var index: mutable.LinkedHashMap[Int, Data_mcsky], var MC: mutable.HashMap[Int, PmcskyCluster], var PD: mutable.HashSet[Int])

class PmcSky(c_queries: ListBuffer[Query]) {

  @transient private var counter: Int = _
  @transient private var cpu_time: Long = 0L

  val queries: ListBuffer[Query] = c_queries
  val slide: Int = queries.head.S

  val R_distinct_list = queries.map(_.R).distinct.sorted
  val k_distinct_list = queries.map(_.k).distinct.sorted
  val R_max = R_distinct_list.max
  val R_min = R_distinct_list.min
  val k_max = k_distinct_list.max
  val k_min = k_distinct_list.min
  val k_size = k_distinct_list.size
  val R_size = R_distinct_list.size

  var mc_counter = 1

  def process(elements: Dataset[(Int, Data_mcsky)], windowEnd: Long, spark: SparkSession, windowStart: Long): Query = {

    //Metrics
    counter += 1
    val time_init = System.currentTimeMillis()

    val inputList = elements.rdd.collect().toList.toIterable

    //create state
    val index = mutable.LinkedHashMap[Int, Data_mcsky]()
    val MC = mutable.HashMap[Int, PmcskyCluster]()
    val PD = mutable.HashSet[Int]()
    val current = PmcskyState(index, MC, PD)

    val all_queries = Array.ofDim[Int](R_size, k_size)

    //insert new elements to state
    inputList
      .filter(_._2.arrival >= windowEnd - slide)
      //Sort is needed when each point has a different timestamp
      //In our case every point in the same slide has the same timestamp
      .toList
      .sortBy(_._2.arrival)
      .foreach(p => current.index += ((p._2.id, p._2)))

    //Update inputList
    inputList.foreach(p => {
      if (!p._2.safe_inlier && p._2.flag == 0 && p._2.mc == -1) {
        checkPoint(p._2, windowEnd, current, windowStart)
        if(p._2.mc == -1){
          if (p._2.lSky.getOrElse(1, ListBuffer()).count(_._2 >= p._2.arrival) >= k_max) p._2.safe_inlier = true
          else{
            var i, y: Int = 0
            var count = p._2.lSky.getOrElse(i+1, ListBuffer()).count(_._2 >= windowStart)
            do{
              if(count >= k_distinct_list(y)){ //inlier for all i
                y += 1
              }else{ //outlier for all y
                for(z <- y until k_size){
                  all_queries(i)(z) += 1
                }
                i += 1
                count += p._2.lSky.getOrElse(i+1, ListBuffer()).count(_._2 >= windowStart)
              }
            }while(i < R_size && y < k_size)
          }
        }
      }
    })

    var tmpQuery = Query(R_distinct_list(0),k_distinct_list(0),queries.head.W,queries.head.S,all_queries(0)(0))
    for (i <- 1 until R_size){
      for (y <- 1 until k_size){
        tmpQuery = Query(R_distinct_list(i),k_distinct_list(y),queries.head.W,queries.head.S,all_queries(i)(y))
      }
    }

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

  def checkPoint(el: Data_mcsky, windowEnd: Long, current: PmcskyState, windowStart: Long): Unit = {
    if (el.lSky.isEmpty && el.mc == -1) { //It's a new point!
      insertPoint(el, current)
    } else if (el.mc == -1) { //It's an old point
      updatePoint(el, windowEnd, current, windowStart)
    }
  }

  def insertPoint(el: Data_mcsky, current: PmcskyState): Unit = {
    //Check against MCs on 3 / 2 * R_max
    val closeMCs = findCloseMCs(el, current)
    //Check if closer MC is within R_min / 2
    val closerMC = if (closeMCs.nonEmpty)
      closeMCs.minBy(_._2)
    else
      (0, Double.MaxValue)
    if (closerMC._2 <= R_min / 2) { //Insert element to MC
      insertToMC(el, closerMC._1, current)
    }
    else { //Check against PD
      val NC = ListBuffer[Data_mcsky]() //List to hold points for new cluster formation
      current.index.values.toList.reverse //get the points so far from latest to earliest
        .takeWhile(p => {
        var res = true
        if (p.id != el.id) {
          if (closeMCs.keySet.contains(p.mc) || p.mc == -1) { //check only the points in PD and close MCs
            val thisDistance = distance(el.value.toArray, p.value.toArray)
            if (thisDistance <= R_max) {
              val skyRes = neighborSkyband(el, p, thisDistance)
              if (!skyRes && thisDistance <= R_min) res = false
              if (p.mc == -1 && thisDistance <= R_min / 2) NC += p
            }
          }
        }
        res
      })
      if (NC.size >= k_max) { //Create new MC
        createNewMC(el, NC, current)
      }
      else { //Insert in PD
        current.PD += el.id
      }
    }
  }

  def updatePoint(el: Data_mcsky, windowEnd: Long, current: PmcskyState, windowStart: Long): Unit = {
    //Remove old points from lSky
    el.lSky.keySet.foreach(p => el.lSky.update(p, el.lSky(p).filter(_._2 >= windowStart)))
    //Create input
    val old_sky = el.lSky.values.flatten.toList.sortWith((p1, p2) => p1._2 > p2._2).map(_._1)
    el.lSky.clear()

    var res = true //variable to stop skyband loop
    val NC = ListBuffer[Data_mcsky]() //List to hold points for new cluster formation
    current.index.values.toList.reverse //Check new points
      .takeWhile(p => {
      var tmpRes = true
      if (p.arrival >= windowEnd - slide) {
        val thisDistance = distance(el.value.toArray, p.value.toArray)
        if (thisDistance <= R_max) {
          val skyRes = neighborSkyband(el, p, thisDistance)
          if (!skyRes && thisDistance <= R_min) res = false
          if (current.PD.contains(p.id) && thisDistance <= R_min / 2) NC += p
        }
      } else tmpRes = false
      res && tmpRes
    })

    if (res)
      old_sky.foreach(l => { //Check the old skyband inputList
        val p = current.index(l)
        val thisDistance = distance(el.value.toArray, p.value.toArray)
        if (thisDistance <= R_max) {
          val skyRes = neighborSkyband(el, p, thisDistance)
          if (!skyRes && thisDistance <= R_min) res = false
          if (current.PD.contains(p.id) && thisDistance <= R_min / 2) NC += p
        }
      })
    if (NC.size >= k_max) createNewMC(el, NC, current) //Create new MC
    else current.PD += el.id //Insert in PD
  }

  def findCloseMCs(el: Data_mcsky, current: PmcskyState): mutable.HashMap[Int, Double] = {
    val res = mutable.HashMap[Int, Double]()
    current.MC.foreach(mc => {
      val thisDistance = distance(el.value.toArray, mc._2.center.toArray)
      if (thisDistance <= (3 * R_max) / 2) res.+=((mc._1, thisDistance))
    })
    res
  }

  def insertToMC(el: Data_mcsky, mc: Int, current: PmcskyState): Unit = {
    el.clear(mc)
    current.MC(mc).points += el.id
    current.PD.remove(el.id)
  }

  def createNewMC(el: Data_mcsky, NC: ListBuffer[Data_mcsky], current: PmcskyState): Unit = {
    NC += el
    NC.foreach(p => {
      p.clear(mc_counter)
      current.PD.remove(p.id)
    })
    val newMC = new PmcskyCluster(el.value, NC.map(_.id))
    current.MC += ((mc_counter, newMC))
    mc_counter += 1
  }

  def deletePoint(el: Data_mcsky, current: PmcskyState): Unit = {
    if (el.mc == -1) { //Delete it from PD
      current.PD.remove(el.id)
    } else {
      current.MC(el.mc).points -= el.id
      if (current.MC(el.mc).points.size <= k_max) {
        current.MC(el.mc).points.foreach(p => {
          current.index(p).clear(-1)
        })
        current.MC.remove(el.mc)
      }
    }
    current.index.remove(el.id)
  }

  def neighborSkyband(el: Data_mcsky, neigh: Data_mcsky, distance: Double): Boolean = {
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
