package single_query

import models.Data_mcod
import utils.Helpers.distance
import utils.Utils.Query

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

case class MicroCluster(var center: ListBuffer[Double], var points: ListBuffer[Data_mcod])

case class PmcodState(var PD: mutable.HashMap[Int, Data_mcod], var MC: mutable.HashMap[Int, MicroCluster])

class Pmcod(c_query: Query) {

  val query: Query = c_query
  val slide: Int = query.S
  val R: Double = query.R
  val k: Int = query.k
  var state: PmcodState = PmcodState(mutable.HashMap[Int, Data_mcod](), mutable.HashMap[Int, MicroCluster]())
  var mc_counter: Int = 1
  @transient private var counter: Int = _
  @transient private var cpu_time: Long = 0L

  def process(elements: ListBuffer[(Int, Data_mcod)], windowEnd: Long, windowStart: Long): (Query, Long) = {

    val time_init = System.nanoTime()
    val PD = mutable.HashMap[Int, Data_mcod]()
    val MC = mutable.HashMap[Int, MicroCluster]()
    state = PmcodState(PD, MC)

    val inputList = elements.toList
    //insert new elements
    inputList
      .filter(p => p._2.c_point.c_flag != 2)
      .foreach(p => {
        insertPoint(p._2, true
        )
      })

    //Find outliers
    var outliers = 0
    state.PD.values.foreach(p => {
      if (!p.safe_inlier && p.flag == 0)
        if (p.count_after + p.nn_before.count(_ >= windowStart) < k) {
          outliers += 1
        }
    })

    val tmpQuery = Query(query.R, query.k, query.W, query.S, outliers)

    //Metrics
    val time_final = System.nanoTime()
    cpu_time = (time_final - time_init)/1000000
    (tmpQuery,cpu_time)
  }

  def insertPoint(el: Data_mcod, newPoint: Boolean, reinsert: ListBuffer[Int] = null): Unit = {
    var state = this.state
    if (!newPoint) el.clear(-1)
    //Check against MCs on 3/2R
    val closeMCs = findCloseMCs(el)
    //Check if closer MC is within R/2
    val closerMC = if (closeMCs.nonEmpty)
      closeMCs.minBy(_._2)
    else
      (0, Double.MaxValue)
    if (closerMC._2 <= R / 2) { //Insert element to MC
      if (newPoint) {
        insertToMC(el, closerMC._1, true)
      }
      else {
        insertToMC(el, closerMC._1, false, reinsert)
      }
    }
    else { //Check against PD
      val NC = ListBuffer[Data_mcod]()
      val NNC = ListBuffer[Data_mcod]()
      state.PD.values
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
        createMC(el, NC, NNC)
      }
      else { //Insert in PD
        closeMCs.foreach(mc => el.Rmc += mc._1)
        state.MC.filter(mc => closeMCs.contains(mc._1))
          .foreach(mc => {
            mc._2.points.foreach(p => {
              val thisDistance = distance(el.value.toArray, p.value.toArray)
              if (thisDistance <= R) {
                addNeighbor(el, p)
              }
            })
          })
        state.PD += ((el.id, el))
      }
    }
  }

  def createMC(el: Data_mcod, NC: ListBuffer[Data_mcod], NNC: ListBuffer[Data_mcod]): Unit = {
    NC.foreach(p => {
      p.clear(mc_counter)
      state.PD.remove(p.id)
    })
    el.clear(mc_counter)
    NC += el
    val newMC = MicroCluster(el.value, NC)
    state.MC += ((mc_counter, newMC))
    NNC.foreach(p => p.Rmc += mc_counter)
    mc_counter += 1
  }

  def insertToMC(el: Data_mcod, mc: Int, update: Boolean, reinsert: ListBuffer[Int] = null): Unit = {
    el.clear(mc)
    state.MC(mc).points += el
    if (update) {
      state.PD.values.filter(p => p.Rmc.contains(mc)).foreach(p => {
        if (distance(p.value.toArray, el.value.toArray) <= R) {
          addNeighbor(p, el)
        }
      })
    }
    else {
      state.PD.values.filter(p => p.Rmc.contains(mc) && reinsert.contains(p.id)).foreach(p => {
        if (distance(p.value.toArray, el.value.toArray) <= R) {
          addNeighbor(p, el)
        }
      })
    }
  }

  def addNeighbor(el: Data_mcod, neigh: Data_mcod): Unit = {
    if (el.arrival > neigh.arrival) {
      el.insert_nn_before(neigh.arrival, k)
    } else {
      el.count_after += 1
      if (el.count_after >= k) el.safe_inlier = true
    }
  }

  def findCloseMCs(el: Data_mcod): mutable.HashMap[Int, Double] = {
    val res = mutable.HashMap[Int, Double]()
    state.MC.foreach(mc => {
      val thisDistance = distance(el.value.toArray, mc._2.center.toArray)
      if (thisDistance <= (3 * R) / 2) res.+=((mc._1, thisDistance))
    })
    res
  }

  def deletePoint(el: Data_mcod): Int = {
    var res = 0
    if (el.mc <= 0) { //Delete it from PD
      state.PD.remove(el.id)
    } else {
      state.MC(el.mc).points -= el
      if (state.MC(el.mc).points.size <= k) res = el.mc
    }
    res
  }
}