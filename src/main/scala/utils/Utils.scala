package utils

import java.lang

import models.{Data_advanced, Data_naive}
import org.apache.spark.sql.{Dataset, SparkSession}
import utils.Helpers._

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object Utils {

  case class Query(R: Double, k: Int, W: Int, S: Int, var outliers: Int)

  case class Metadata_naive(var outliers: mutable.HashMap[Int, Data_naive])

  case class Metadata_advanced(var outliers: mutable.HashMap[Int, Data_advanced])

  class GroupMetadataNaive(c_query: Query) {

    val query: Query = c_query
    val W: Int = query.W
    val slide: Int = query.S
    val R: Double = query.R
    val k: Int = query.k

    def process(elements: ListBuffer[Data_naive],windowStart: Int): Query = {
      var newMap = mutable.HashMap[Int, Data_naive]()
      //all elements are new to the window so we have to combine the same ones
      //and add them to the map
      for (el <- elements) {
        val oldEl: Data_naive = newMap.getOrElse(el.id, null)
        if (oldEl == null) {
          newMap += ((el.id, el))
        } else {
          val newValue = combine_elements(oldEl, el, k)
          newMap += ((el.id, newValue))
        }
      }
      var current = Metadata_naive(newMap)

      var outliers = 0
      for (el <- current.outliers.values) {
        val nnBefore = el.nn_before.count(_ >= windowStart)
        if (nnBefore + el.count_after < k) outliers += 1
      }
      val tmpQuery = Query(query.R, query.k, query.W, query.S, outliers)
      tmpQuery
    }
  }

  class GroupMetadataAdvanced(c_query: Query) {

    val query: Query = c_query
    val W: Int = query.W
    val slide: Int = query.S
    val R: Double = query.R
    val k: Int = query.k

    def process(elements: scala.Iterable[Data_advanced], windowEnd: Long, spark: SparkSession): Query = {
      var newMap = mutable.HashMap[Int, Data_advanced]()
      //all elements are new to the window so we have to combine the same ones
      //and add them to the map
      for (el <- elements) {
        val oldEl = newMap.getOrElse(el.id, null)
        val newValue = combine_new_elements(oldEl, el, k)
        newMap += ((el.id, newValue))
      }
      var current = Metadata_advanced(newMap)
      if (current == null) { //populate list for the first time
        var newMap = mutable.HashMap[Int, Data_advanced]()
        //all elements are new to the window so we have to combine the same ones
        //and add them to the map
        for (el <- elements) {
          val oldEl = newMap.getOrElse(el.id, null)
          val newValue = combine_new_elements(oldEl, el, k)
          newMap += ((el.id, newValue))
        }
        current = Metadata_advanced(newMap)
      } else { //update list

        //first remove old elements
        var forRemoval = ListBuffer[Int]()
        for (el <- current.outliers.values) {
          if (el.arrival < windowEnd - W) {
            forRemoval = forRemoval.+=(el.id)
          }
        }
        forRemoval.foreach(el => current.outliers -= el)
        //then insert or combine elements
        for (el <- elements) {
          val oldEl = current.outliers.getOrElse(el.id, null)
          if (el.arrival >= windowEnd - slide) {
            val newValue = combine_new_elements(oldEl, el, k)
            current.outliers += ((el.id, newValue))
          } else {
            if (oldEl != null) {
              val newValue = combine_old_elements(oldEl, el, k)
              current.outliers += ((el.id, newValue))
            }
          }
        }
      }

      var outliers = 0
      for (el <- current.outliers.values) {
        if (!el.safe_inlier) {
          val nnBefore = el.nn_before.count(_ >= windowEnd - W)
          if (nnBefore + el.count_after < k) outliers += 1
        }
      }
      val tmpQuery = Query(query.R,query.k,query.W,query.S,outliers)
      tmpQuery
    }
  }

  class PrintOutliers {
    def process(elements: scala.Iterable[(Long, Query)]): Unit = {
      val group_outliers = elements
        .map(record => ((record._2.W,record._2.S,record._2.R,record._2.k), record._2.outliers))
        .foldLeft(Map[(Int,Int,Double,Int), Int]().withDefaultValue(0))((res, v) => {
          val key = v._1
          res + (key -> (res(key) + v._2))
        })
      group_outliers.foreach(record => println(s"${record._1};${record._2}"))
    }
  }

  class WriteOutliers {
    def process(elements: scala.Iterable[(Long, Query)]): Unit = {
      val group_outliers = elements
        .map(record => ((record._2.W,record._2.S,record._2.R,record._2.k), record._2.outliers))
        .foldLeft(Map[(Int,Int,Double,Int), Int]().withDefaultValue(0))((res, v) => {
          val key = v._1
          res + (key -> (res(key) + v._2))
        })

      /*val measurement = "outliers"
      val timestamp = key
      group_outliers.foreach(record => {
        val tags = Map[String, String]("W"->record._1._1.toString, "S"->record._1._2.toString, "k"->record._1._4.toString, "R"->record._1._3.toString).asJava
        val fields = Map[String, Object]("Outliers"->record._2.asInstanceOf[Object]).asJava
        out.collect(new InfluxDBPoint(measurement, timestamp, tags, fields))
      })*/

    }
  }

}
