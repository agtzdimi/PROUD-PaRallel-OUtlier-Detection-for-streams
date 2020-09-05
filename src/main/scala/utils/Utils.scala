package utils

import java.lang

import models.{Data_advanced, Data_naive}
import utils.Helpers._
import scala.collection.JavaConverters._

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object Utils {

  case class Query(R: Double, k: Int, W: Int, S: Int, var outliers: Int)

  case class Metadata_naive(var outliers: mutable.HashMap[Int, Data_naive])

  case class Metadata_advanced(var outliers: mutable.HashMap[Int, Data_advanced])

}
