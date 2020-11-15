package single_query

import utils.Utils.Query
import mtree.{utils, _}
import models.{Data_advanced}
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

case class AdvancedState(var tree: MTree[Data_advanced], var hashMap: mutable.HashMap[Int, Data_advanced])

class Advanced(c_query: Query) {

  @transient private var counter: Int = _
  @transient private var cpu_time: Long = 0L

  val slide: Int = c_query.S
  val R: Double = c_query.R
  val k: Int = c_query.k

  def process(elements: Dataset[(Int, Data_advanced)], windowEnd: Long, spark: SparkSession):scala.Iterable[Data_advanced] = {

    //Metrics
    counter += 1
    val time_init = System.currentTimeMillis()

    val window = windowEnd
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
    val inputList = elements.rdd.collect().toList.toIterable
    val mySplit = new ComposedSplitFunction[Data_advanced](nonRandomPromotion, new PartitionFunctions.BalancedPartition[Data_advanced])
    val myTree = new MTree[Data_advanced](k, DistanceFunctions.EUCLIDEAN, mySplit)
    var myHash = new mutable.HashMap[Int, Data_advanced]()
    for (el <- inputList) {
      myTree.add(el._2)
      myHash.+=((el._2.id, el._2))
    }
    var current = AdvancedState(myTree, myHash)
    //populate Mtree
    if (current == null) {
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
      current = AdvancedState(myTree, myHash)
    } else {
      inputList
        .filter(el => el._2.arrival >= window - slide)
        .foreach(el => {
          current.tree.add(el._2)
          current.hashMap.+=((el._2.id, el._2))
        })
    }

    //Get neighbors
    inputList
      .filter(p => p._2.arrival >= (window - slide))
      .foreach(p => {
        val tmpData = p._2
        val query: MTree[Data_advanced]#Query = current.tree.getNearestByRange(tmpData, R)
        val iter = query.iterator()
        while (iter.hasNext) {
          val node = iter.next().data
          if (node.id != tmpData.id) {
            if (node.arrival < (window - slide)) {
              current.hashMap(tmpData.id).insert_nn_before(node.arrival, k)
              current.hashMap(node.id).count_after += 1
              if (current.hashMap(node.id).count_after >= k)
                current.hashMap(node.id).safe_inlier = true
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

    var iter = ListBuffer[Data_advanced]();
    current.hashMap.values.foreach(p => {
      iter += p
    })

    //Remove expiring objects from tree and flagged ones
    inputList
      .filter(el => el._2.arrival < window + slide || el._2.flag == 1)
      .foreach(el => {
        current.tree.remove(el._2)
        current.hashMap.-=(el._2.id)
      })
    //update state

    //Metrics
    val time_final = System.currentTimeMillis()
    cpu_time += (time_final - time_init)

    return iter.toIterable
  }

}
