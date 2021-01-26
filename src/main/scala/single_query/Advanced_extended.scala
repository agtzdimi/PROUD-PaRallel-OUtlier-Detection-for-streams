package single_query

import models.Data_advanced
import utils.Utils.Query
import mtree.{utils, _}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

case class AdvancedExtState(var tree: MTree[Data_advanced], var hashMap: mutable.HashMap[Int, Data_advanced])

class Advanced_extended(c_query: Query) {

  val query: Query = c_query
  val slide: Int = query.S
  val R: Double = query.R
  val k: Int = query.k
  @transient private var counter: Int = _
  @transient private var cpu_time: Long = 0L

  def process(elements: ListBuffer[(Int, Data_advanced)], windowEnd: Long, windowStart: Long): (Query, Long) = {

    //Metrics
    counter += 1
    val time_init = System.currentTimeMillis()

    val inputList = elements

    //populate Mtree
    val nonRandomPromotion = new PromotionFunction[Data_advanced] {
      /**
        * Chooses (promotes) a pair of objects according to some criteria that is
        * suitable for the application using the M-Tree.
        *
        * @param dataSet          The set of objects to choose a pair from.
        * @param distanceFunction A function that can be used for choosing the
        *                         promoted objects.
        * @return A pair of chosen objects.
        */
      override def process(dataSet: java.util.Set[Data_advanced], distanceFunction: DistanceFunction[_ >: Data_advanced]): utils.Pair[Data_advanced] = {
        utils.Utils.minMax[Data_advanced](dataSet)
      }
    }
    val mySplit = new ComposedSplitFunction[Data_advanced](nonRandomPromotion, new PartitionFunctions.BalancedPartition[Data_advanced])
    val myTree = new MTree[Data_advanced](k, DistanceFunctions.EUCLIDEAN, mySplit)
    var myHash = new mutable.HashMap[Int, Data_advanced]()
    for (el <- inputList) {
      myTree.add(el._2)
      myHash.+=((el._2.id, el._2))
    }
    val current = AdvancedExtState(myTree, myHash)

    //Get neighbors
    inputList
      .foreach(p => {
        val tmpData = p._2
        val query: MTree[Data_advanced]#Query = current.tree.getNearestByRange(tmpData, R)
        val iter = query.iterator()
        while (iter.hasNext) {
          val node = iter.next().data
          if (node.id != tmpData.id) {
            if (node.arrival < (windowEnd - slide)) {
              if (tmpData.flag == 0) {
                current.hashMap(tmpData.id).insert_nn_before(node.arrival, k)
              }
              if (node.flag == 0) {
                current.hashMap(node.id).count_after += 1
                if (current.hashMap(node.id).count_after >= k)
                  current.hashMap(node.id).safe_inlier = true
              }
            } else {
              if (tmpData.flag == 0) {
                current.hashMap(tmpData.id).count_after += 1
                if (current.hashMap(tmpData.id).count_after >= k)
                  current.hashMap(tmpData.id).safe_inlier = true
              }
            }
          }
        }
      })

    //Variable for number of outliers
    var outliers = 0

    current.hashMap.values.foreach(p => {
      if (p.flag == 0 && !p.safe_inlier) {
        val nnBefore = p.nn_before.count(_ >= windowStart)
        if (p.count_after + nnBefore < k) outliers += 1
      }
    })

    val tmpQuery = Query(query.R, query.k, query.W, query.S, outliers)

    var iter = ListBuffer[Data_advanced]();

    //Metrics
    val time_final = System.currentTimeMillis()
    cpu_time = time_final - time_init
    (tmpQuery, cpu_time)
  }

}
