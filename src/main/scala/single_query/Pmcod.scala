package single_query

import utils.Utils.Query
import utils.Helpers.distance
import models.{Data_advanced, Data_mcod}
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

case class MicroCluster(var center: ListBuffer[Double], var points: ListBuffer[Data_mcod])
case class PmcodState(var PD: mutable.HashMap[Int, Data_mcod], var MC: mutable.HashMap[Int, MicroCluster])

class Pmcod(c_query: Query) {

  @transient private var counter: Int = _
  @transient private var cpu_time: Long = 0L

  val query: Query = c_query
  val slide: Int = query.S
  val R: Double = query.R
  val k: Int = query.k
  var mc_counter: Int = 1

  def process(elements: Dataset[(Int, Data_mcod)], windowEnd: Long, spark: SparkSession, windowStart: Long):Query = {

    //Metrics
    counter += 1
    val time_init = System.currentTimeMillis()

    val inputList = elements.rdd.collect().toList.toIterable
    //create state
    val PD = mutable.HashMap[Int, Data_mcod]()
    val MC = mutable.HashMap[Int, MicroCluster]()
    val current = PmcodState(PD, MC)

    //insert new elements
    inputList
      .filter(_._2.arrival >= windowEnd - slide)
      .foreach(p => insertPoint(p._2, true,null, current))

    //Find outliers
    var outliers = 0
    current.PD.values.foreach(p => {
      if (!p.safe_inlier && p.flag == 0)
        if (p.count_after + p.nn_before.count(_ >= windowEnd) < k) {
          outliers += 1
        }
    })

    val tmpQuery = Query(query.R,query.k,query.W,query.S,outliers)

    //Remove old points
    var deletedMCs = mutable.HashSet[Int]()
    inputList
      .filter(p => p._2.arrival < windowStart + slide)
      .foreach(p => {
        val delete = deletePoint(p._2, current)
        if (delete > 0) deletedMCs += delete
      })

    //Delete MCs
    if (deletedMCs.nonEmpty) {
      var reinsert = ListBuffer[Data_mcod]()
      deletedMCs.foreach(mc => {
        reinsert = reinsert ++ current.MC(mc).points
        current.MC.remove(mc)
      })
      val reinsertIndexes = reinsert.map(_.id)

      //Reinsert points from deleted MCs
      reinsert.foreach(p => insertPoint(p, false, reinsertIndexes, current))
    }

    //Metrics
    val time_final = System.currentTimeMillis()
    cpu_time += (time_final - time_init)
    tmpQuery
  }

  def insertPoint(el: Data_mcod, newPoint: Boolean, reinsert: ListBuffer[Int] = null, current: PmcodState): Unit = {
    if (!newPoint) el.clear(-1)
    //Check against MCs on 3/2R
    val closeMCs = findCloseMCs(el,current)
    //Check if closer MC is within R/2
    val closerMC = if (closeMCs.nonEmpty)
      closeMCs.minBy(_._2)
    else
      (0, Double.MaxValue)
    if (closerMC._2 <= R / 2) { //Insert element to MC
      if (newPoint) {
        insertToMC(el, closerMC._1, true, null, current)
      }
      else {
        insertToMC(el, closerMC._1, false, reinsert, current)
      }
    }
    else { //Check against PD
      val NC = ListBuffer[Data_mcod]()
      val NNC = ListBuffer[Data_mcod]()
      current.PD.values
        .foreach(p => {
          val thisDistance = distance(el.value.toArray, p.value.toArray)
          if (thisDistance <= 3 * R / 2) {
            if (thisDistance <= R) { //Update metadata
              addNeighbor(el, p)
              if (newPoint) {
                addNeighbor(p, el)
              }
              else {
                if (reinsert.contains(p.id)) {
                  addNeighbor(p, el)
                }
              }
            }
            if (thisDistance <= R / 2) NC += p
            else NNC += p
          }
        })

      if (NC.size >= k) { //Create new MC
        createMC(el, NC, NNC, current)
        println(el)
      }
      else { //Insert in PD
        closeMCs.foreach(mc => el.Rmc += mc._1)
        current.MC.filter(mc => closeMCs.contains(mc._1))
          .foreach(mc => {
            mc._2.points.foreach(p => {
              val thisDistance = distance(el.value.toArray, p.value.toArray)
              if (thisDistance <= R) {
                addNeighbor(el, p)
              }
            })
          })
        current.PD += ((el.id, el))
        println(el)
      }
    }
  }

  def deletePoint(el: Data_mcod, current: PmcodState): Int = {
    var res = 0
    if (el.mc <= 0) { //Delete it from PD
      current.PD.remove(el.id)
    } else {
      current.MC(el.mc).points -= el
      if (current.MC(el.mc).points.size <= k) res = el.mc
    }
    res
  }

  def createMC(el: Data_mcod, NC: ListBuffer[Data_mcod], NNC: ListBuffer[Data_mcod], current: PmcodState): Unit = {
    NC.foreach(p => {
      p.clear(mc_counter)
      current.PD.remove(p.id)
    })
    el.clear(mc_counter)
    NC += el
    val newMC = MicroCluster(el.value, NC)
    current.MC += ((mc_counter, newMC))
    NNC.foreach(p => p.Rmc += mc_counter)
    mc_counter += 1
  }

  def insertToMC(el: Data_mcod, mc: Int, update: Boolean, reinsert: ListBuffer[Int] = null, current: PmcodState): Unit = {
    el.clear(mc)
    current.MC(mc).points += el
    if (update) {
      current.PD.values.filter(p => p.Rmc.contains(mc)).foreach(p => {
        if (distance(p.value.toArray, el.value.toArray) <= R) {
          addNeighbor(p, el)
        }
      })
    }
    else {
      current.PD.values.filter(p => p.Rmc.contains(mc) && reinsert.contains(p.id)).foreach(p => {
        if (distance(p.value.toArray, el.value.toArray) <= R) {
          addNeighbor(p, el)
        }
      })
    }
  }

  def findCloseMCs(el: Data_mcod, current: PmcodState): mutable.HashMap[Int, Double] = {
    val res = mutable.HashMap[Int, Double]()
    current.MC.foreach(mc => {
      val thisDistance = distance(el.value.toArray, mc._2.center.toArray)
      if (thisDistance <= (3 * R) / 2) res.+=((mc._1, thisDistance))
    })
    res
  }

  def addNeighbor(el: Data_mcod, neigh: Data_mcod): Unit = {
    if (el.arrival > neigh.arrival) {
      el.insert_nn_before(neigh.arrival, k)
    } else {
      el.count_after += 1
      if (el.count_after >= k) el.safe_inlier = true
    }
  }
}
