package single_query

import utils.Utils.Query
import mtree.{utils, _}
import models.{Data_advanced, Data_slicing}
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

case class SlicingState(var trees: mutable.HashMap[Long, MTree[Data_slicing]], var triggers: mutable.HashMap[Long, mutable.Set[Int]])

class Slicing(c_query: Query) {

  @transient private var counter: Int = _
  @transient private var cpu_time: Long = 0L

  val query: Query = c_query
  val slide: Int = query.S
  val R: Double = query.R
  val k: Int = query.k
  val outliers_trigger: Long = -1L

  def process(elements: ListBuffer[(Int, Data_slicing)], windowEnd: Long, windowStart: Long):(Query,Long) = {

    //Metrics
    counter += 1
    val time_init = System.currentTimeMillis()

    val inputList = elements
    //new variables
    val latest_slide = windowEnd - slide
    val nonRandomPromotion = new PromotionFunction[Data_slicing] {
      /**
        * Chooses (promotes) a pair of objects according to some criteria that is
        * suitable for the application using the M-Tree.
        *
        * @param dataSet          The set of objects to choose a pair from.
        * @param distanceFunction A function that can be used for choosing the
        *                         promoted objects.
        * @return A pair of chosen objects.
        */
      override def process(dataSet: java.util.Set[Data_slicing], distanceFunction: DistanceFunction[_ >: Data_slicing]): utils.Pair[Data_slicing] = {
        utils.Utils.minMax[Data_slicing](dataSet)
      }
    }
    val mySplit = new ComposedSplitFunction[Data_slicing](nonRandomPromotion, new PartitionFunctions.BalancedPartition[Data_slicing])
    val myTree = new MTree[Data_slicing](k, DistanceFunctions.EUCLIDEAN, mySplit)

    var myTrigger = mutable.HashMap[Long, mutable.Set[Int]]()
    myTrigger.+=((outliers_trigger, mutable.Set()))
    var next_slide = windowStart
    while(next_slide <= windowEnd - slide){
      myTrigger.+=((next_slide, mutable.Set()))
      next_slide += slide
    }
    for (el <- inputList) {
      myTree.add(el._2)
    }
    val myTrees = mutable.HashMap[Long, MTree[Data_slicing]]((latest_slide, myTree))
    var current = SlicingState(myTrees, myTrigger)

    //Trigger leftover slides
    val slow_triggers = current.triggers.keySet.filter(p => p < windowStart && p!= -1L).toList
    for(slow <- slow_triggers){
      val slow_triggers_points = current.triggers(slow).toList
      inputList
        .filter(p => slow_triggers_points.contains(p._2.id))
        .foreach(p =>trigger_point(p._2, windowEnd, current, windowStart))
      current.triggers.remove(slow)
    }

    //Insert new points
    inputList
      .filter(p => p._2.arrival >= windowEnd - slide && p._2.flag == 0)
      .foreach(p => {
        insert_point(p._2, windowEnd, current, windowStart)
      })

    //Trigger previous outliers
    val triggered_outliers = current.triggers(outliers_trigger).toList
    current.triggers(outliers_trigger).clear()
    inputList
      .filter(p => triggered_outliers.contains(p._2.id))
      .foreach(p =>trigger_point(p._2, windowEnd, current, windowStart))

    //Report outliers
    val outliers = inputList.count(p => {
      p._2.flag == 0 &&
        !p._2.safe_inlier &&
        p._2.count_after + p._2.slices_before.filter(_._1 >= windowStart).values.sum < k
    })

    val tmpQuery = Query(query.R,query.k,query.W,query.S,outliers)

    var iter = ListBuffer[Data_slicing]();
    //Trigger expiring list
    current.trees.remove(windowStart)
    val triggered: List[Int] = current.triggers(windowStart).toList
    current.triggers.remove(windowStart)
    inputList
      .filter(p => triggered.contains(p._2.id))
      .foreach(p =>trigger_point(p._2, windowStart, current, windowEnd))

    //Metrics
    val time_final = System.currentTimeMillis()
    cpu_time = time_final - time_init

    (tmpQuery,cpu_time)
  }

  def trigger_point(point: Data_slicing, windowEnd: Long, current: SlicingState, windowStart: Long): Unit = {
    var next_slide = //find starting slide
      if (point.last_check != 0L) point.last_check + slide
      else get_slide(point.arrival, windowStart) + slide
    //Find no of neighbors
    var neigh_counter = point.count_after +
      point.slices_before.filter(_._1 >= windowStart + slide).values.sum
    while (neigh_counter < k && next_slide <= windowEnd - slide) {
      val myTree = current.trees.getOrElse(next_slide, null)
      if (myTree != null) {
        val query: MTree[Data_slicing]#Query = myTree.getNearestByRange(point, R)
        val iter = query.iterator()
        //Update point's metadata
        while (iter.hasNext) {
          iter.next()
          point.count_after += 1
          neigh_counter += 1
        }
        if (point.count_after >= k) point.safe_inlier = true
      }
      point.last_check = next_slide
      next_slide += slide
    }
    if (neigh_counter < k) {
      current.triggers(outliers_trigger).+=(point.id)
    }
  }

  def insert_point(point: Data_slicing, windowEnd: Long, current: SlicingState, windowStart: Long): Unit = {
    var (neigh_counter, next_slide) = (0, windowEnd - slide)
    while (neigh_counter < k && next_slide >= windowStart) { //Query each slide's MTREE
      val myTree = current.trees.getOrElse(next_slide, null)
      if (myTree != null) {
        val query: MTree[Data_slicing]#Query = myTree.getNearestByRange(point, R)
        val iter = query.iterator()
        //If it has neighbors insert it into the slide's trigger
        if (iter.hasNext)
          current.triggers(next_slide).+=(point.id)
        //Update point's metadata
        while (iter.hasNext) {
          val node = iter.next().data
          if (next_slide == windowEnd - slide) {
            if (node.id != point.id) {
              point.count_after += 1
              neigh_counter += 1
            }
          } else {
            point.slices_before.update(next_slide, point.slices_before.getOrElse(next_slide, 0) + 1)
            neigh_counter += 1
          }
        }
        if (next_slide == windowEnd - slide && neigh_counter >= k) point.safe_inlier = true
      }
      next_slide -= slide
    }
    //If it is an outlier insert into trigger list
    if (neigh_counter < k) current.triggers(outliers_trigger).+=(point.id)
  }

  def get_slide(arrival: Long, windowStart: Long): Long = {
    val first = arrival - windowStart
    val div = first / slide
    val int_div = div.toInt
    windowStart + (int_div * slide)
  }
}

