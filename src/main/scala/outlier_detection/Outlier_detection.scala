package outlier_detection

import java.sql.Timestamp
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter
import java.time.temporal.{ChronoField, TemporalAccessor}

import com.github.fsanaulla.chronicler.core.model.{InfluxCredentials, InfluxWriter}
import com.github.fsanaulla.chronicler.spark.core.CallbackHandler
import com.github.fsanaulla.chronicler.urlhttp.shared.InfluxConfig
import models._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.current_timestamp
import org.apache.spark.sql.streaming.StreamingQueryListener.{QueryProgressEvent, QueryStartedEvent, QueryTerminatedEvent}
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode, StreamingQueryListener, Trigger}
import org.apache.spark.sql.{Row, SparkSession, functions}
import org.apache.spark.{SparkConf, SparkContext}
import partitioning.Grid.grid_partitioning
import partitioning.Replication.replication_partitioning
import partitioning.Tree.Tree_partitioning
import scopt.OParser
import single_query.{Advanced_extended, Slicing}
import utils.Helpers.find_gcd
import utils.Utils.Query

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object Outlier_detection {

  val delimiter = ","
  val line_delimeter = '&'
  var myQueriesGlobal = new ListBuffer[Query]()
  var windowStart: Long = 0
  var windowEnd: Long = 500
  var windowSize: Int = 10000
  var slideSize: Int = 500

  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    case class Config(
                       DEBUG: Boolean = false,
                       dataset: String = "",
                       space: String = "",
                       checkPointDir: String = "",
                       algorithm: String = "",
                       W: List[Int] = List.empty[Int],
                       S: ListBuffer[Int] = ListBuffer.empty[Int],
                       k: List[Int] = List.empty[Int],
                       R: List[Double] = List.empty[Double],
                       partitioning: String = "",
                       tree_init: Int = 10000,
                       kwargs: Map[String, String] = Map())


    val builder = OParser.builder[Config]


    val parser = {
      import builder._
      OParser.sequence(
        programName("PROUD Spark Streaming"),
        head("scopt", "4.x"),
        opt[Boolean]("DEBUG")
          .action((x, c) => c.copy(DEBUG = x))
          .text("(Optional) DEBUG is a Boolean property"),
        opt[String]("dataset")
          .action((x, c) => c.copy(dataset = x))
          .required()
          .validate(x => {
            if (x == "TAO" || x == "STK" || x == "FC") success
            else failure("dataset property can only be set as [TAO|STK]")
          })
          .text("Represents the dataset selected for the input. Affects the partitioning technique" +
            "It can either be set to 'TAO', 'STK' or 'FC'"),
        opt[String]("space")
          .action((x, c) => c.copy(space = x))
          .required()
          .validate(x => {
            if (x == "single" || x == "rk" || x == "rkws") success
            else failure("space property can only be set as [single|rk|rkws]")
          })
          .text("Represents the query space. Possible values are \"single\" for single-query, \"rk\" for multi-query with multiple application parameters and \"rkws\" for multi-query with both multiple application and windowing parameters"),
        opt[String]("checkPointDir")
          .action((x, c) => c.copy(dataset = x))
          .required()
          .validate(x => {
            if (x.startsWith("/")) success
            else failure("The Checkpoint Directory should be a full path directory starting with '/'")
          })
          .text("checkPointDir contains the full path directory to store the spark checkpoint of the application"),
        opt[String]("algorithm")
          .action((x, c) => c.copy(algorithm = x))
          .required()
          .validate(x => {
            if (x == "naive" || x == "advanced" || x == "advanced_extended" || x == "slicing" ||
              x == "pmcod" || x == "pmcod_net" || x == "amcod" || x == "sop" || x == "psod" || x == "pmcsky") success
            else failure("algorithm property can only be set as [naive|advanced|advanced_extended|slicing|pmcod|pmcod_net|amcod|sop|psod|pmcsky]")
          })
          .text("Represents the outlier detection algorithm. Possible values for single-query space are: \"naive\", \"advanced\", \"advanced_extended\", \"slicing\", \"pmcod\" and \"pmcod_net\". Possible values for multi-query space are \"amcod\", \"sop\", \"psod\" and \"pmcsky\""),
        opt[String]("W")
          .action((x, c) => c.copy(W = x.split(delimiter).toList.map(_.toInt)))
          .required()
          .text("Windowing parameter. Represents the window size. On the multi-query space many values can be given delimited by \";\""),
        opt[String]("S")
          .action((x, c) => c.copy(S = x.split(delimiter).to[ListBuffer].map(_.toInt)))
          .required()
          .text("Windowing parameter. Represents the slide size. On the multi-query space many values can be given delimited by \";\""),
        opt[String]("k")
          .action((x, c) => c.copy(k = x.split(delimiter).toList.map(_.toInt)))
          .required()
          .text("Application parameter. Represents the minimum number of neighbors a data point must have in order to be an inlier. On the multi-query space many values can be given delimited by \";\""),
        opt[String]("R")
          .action((x, c) => c.copy(R = x.split(delimiter).toList.map(_.toDouble)))
          .required()
          .text("Application parameter. Represents the maximum distance that two data points can have to be considered neighbors. On the multi-query space many values can be given delimited by \";\""),
        opt[String]("partitioning")
          .action((x, c) => c.copy(partitioning = x))
          .required()
          .validate(x => {
            if (x == "replication" || x == "grid" || x == "tree") success
            else failure("Represents the partitioning technique chosen for the job. \"replication\" partitioning is mandatory for \"naive\" and \"advanced\" algorithms whilst \"grid\" and \"tree\" partitioning is available for every other algorithm. \"grid\" technique needs pre-recorded data on the dataset's distribution. \"tree\" technique needs a file containing data points from the dataset in order to initialize the VP-tree")
          })
          .text("partitioning can either be set to 'replication'/'grid'/'tree'"),
        opt[Int]("tree_init")
          .action((x, c) => c.copy(tree_init = x))
          .text("(Optional) Represents the number of data points to be read for initialization by the \"tree\" partitioning technique. Default value is 10000")
      )
    }

    var arguments: Config = Config()

    // OParser.parse returns Option[Config]
    OParser.parse(parser, args, Config()) match {
      case Some(config) =>
        arguments = Config(config.DEBUG, config.dataset, config.space, config.checkPointDir, config.algorithm, config.W, config.S, config.k, config.R, config.partitioning, config.tree_init)
      case _ =>
      // arguments are bad, error message will have been displayed
    }
    println(arguments.S)

    //Hardcoded parameter for partitioning and windowing
    val partitions = 16
    // Input file
    val file_input = System.getenv("JOB_INPUT") //File input
    // Comma Separated Topics
    val kafka_topic = System.getenv("KAFKA_TOPIC") //File input

    //Pre-process parameters for initializations
    val common_W = if (arguments.W.length > 1) arguments.W.max else arguments.W.head
    val common_S = if (arguments.S.length > 1) find_gcd(arguments.S) else arguments.S.head
    val common_R = if (arguments.R.length > 1) arguments.R.max else arguments.R.head
    windowSize = common_W
    slideSize = common_S
    windowEnd = slideSize
    //Variable to allow late data points within the specific time be ingested by the job
    val allowed_lateness = common_S

    //Create query/queries
    val myQueries = new ListBuffer[Query]()
    for (w <- arguments.W)
      for (s <- arguments.S)
        for (r <- arguments.R)
          for (k <- arguments.k)
            myQueries += Query(r, k, w, s, 0)

    myQueriesGlobal = myQueries
    //Create the tree for the specified partitioning type
    val myTree = if (arguments.partitioning == "tree") {
      var tree_input = s"/home/dimitris/inputs/splits/treeSTK/tree_input.txt"
      if(arguments.dataset == "TAO") {
        tree_input = s"/home/dimitris/inputs/splits/taoTree/tao_tree_input.txt"
      } else if (arguments.dataset == "FC") {
        tree_input = s"/home/dimitris/inputs/splits/treeFC/tree_input.txt"
      }
      new Tree_partitioning(arguments.tree_init, partitions, tree_input)
    }
    else null

    //Kafka brokers
    val kafka_brokers = System.getenv("KAFKA_BROKERS")

    val spark = SparkSession
      .builder
      .appName("PROUD Spark Streaming")
      .master("local[" + partitions + "]")
      .config("spark.sql.streaming.checkpointLocation", "/home/dimitris/checkPointDir")
      .getOrCreate()

    spark.conf.set("spark.sql.legacy.setCommandRejectsSparkCoreConfs","false")
    spark.conf.set("spark.hadoop.validateOutputSpecs", "false")
    spark.conf.set("spark.sql.shuffle.partitions", 16)
    spark.conf.set("spark.eventLog.dir", "spark-logs")
    spark.conf.set("spark.eventLog.enabled",true)
    /*spark.conf.set("spark.sql.adaptive.enabled", true)*/
    spark.conf.set("spark.streaming.blockInterval", 100)
    /*spark.conf.set("spark.streaming.receiver.maxRate",0)*/
    /*spark.conf.set("spark.shuffle.manager", "tungsten-sort")
    spark.conf.set("spark.shuffle.unsafe.fastMergeEnabled", "true")*/
    spark.conf.set("spark.shuffle.compress", false)
    spark.conf.set("spark.shuffle.spill.compress", false)
    spark.conf.set("spark.kryo.unsafe", true)

    println(arguments.DEBUG)
    println(kafka_brokers)
    println(arguments.algorithm)
    val data = if (arguments.DEBUG) {
      val myInput = s"$file_input"
      val debugFile = spark.readStream
        .format("text")
        .option("maxFilesPerTrigger", 1)
        .load(myInput)
      debugFile
    } else {
      val kafkaDF = spark
        .readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_brokers)
        .option("subscribe", kafka_topic)
        .load()
      kafkaDF
    } // for .toDF() method

    // Publish data to console
    /*   val query = data
         .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "CAST(timestamp AS STRING)")
         .writeStream
         .format("console")
         .start()*/

    val influxdb_host = System.getenv("INFLUXDB_HOST")
    val influxdb_port = System.getenv("INFLUXDB_PORT")
    val influxdb_user = System.getenv("INFLUXDB_USER")
    val influxdb_pass = System.getenv("INFLUXDB_PASSWORD")
    val influxdb_db = System.getenv("INFLUXDB_DB")

    implicit lazy val influxConf: InfluxConfig = InfluxConfig(
      influxdb_host,
      influxdb_port.toInt,
      Some(InfluxCredentials(influxdb_user, influxdb_pass)))

    // InfluxDB Handler
    val handler: Option[CallbackHandler] = Some(
      CallbackHandler(
        code => println(s"OnSuccess code: $code"),
        err => println(s"Error on application level ${err.getMessage}"),
        err => println(s"Error on network level: cause: ${err.getCause}, msg: ${err.getMessage}")
      )
    )

    /*val saveToInflux = data
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "CAST(timestamp AS STRING)")
      .writeStream
      .saveToInfluxDB(influxdb_db, handler)
      .start()*/

    import spark.implicits._

    /*    var key: Int = 0
        val windowedData = data
          //.withColumn("timestamp", lit(current_timestamp()))
          .withColumn("timestamp", $"timestamp".cast(TimestampType))
          .withWatermark("timestamp", s"$common_W millisecond")
          .selectExpr("CAST(value AS STRING)", "CAST(timestamp AS STRING)")*/

    val data2 = data
      .selectExpr("CAST(value AS STRING)")
      .map(record => {
        val id = record.get(0).toString.split(line_delimeter)(0).toInt
        val value = record.get(0).toString
          .split(line_delimeter)(1).toString
          .split(delimiter)
          .map(_.toDouble)
          .to[ListBuffer]
        val timestamp = id.toLong
        new Data_basis(id, value, timestamp, 0)
      })
    //Start partitioning
    val partitioned_data = arguments.partitioning match {
      case "replication" =>
        data2
          .flatMap(record => replication_partitioning(partitions, record))
      case "grid" =>
        data2
          .flatMap(record => grid_partitioning(partitions, record, common_R, arguments.dataset, common_S))
      case "tree" =>
        data2
          .flatMap(record => myTree.tree_partitioning(partitions, record, common_R, common_S))
    }

    val out = arguments.algorithm match {
      case "pmcod" =>
        partitioned_data.map(record => (record._1, new Data_mcod(record._2)))
          .groupByKey(_._1)
          .flatMapGroupsWithState(outputMode = OutputMode.Append(), GroupStateTimeout.ProcessingTimeTimeout)(updatePoints)
          .toDF()
      case "advanced_extended" =>
        partitioned_data.map(record => (record._1, new Data_advanced(record._2)))
          .groupByKey(_._1)
          .flatMapGroupsWithState(outputMode = OutputMode.Append(), GroupStateTimeout.ProcessingTimeTimeout)(updatePointsAdvancedExt)
          .toDF()
      case "slicing" =>
        partitioned_data.map(record => (record._1, new Data_slicing(record._2)))
          .groupByKey(_._1)
          .flatMapGroupsWithState(outputMode = OutputMode.Append(), GroupStateTimeout.ProcessingTimeTimeout)(updatePointsSlicing)
          .toDF()
    }

    val output_data = out
      .withColumn("timestamp", current_timestamp())
      .withWatermark("timestamp", s"${slideSize} millisecond")
      .groupBy("timestamp")
      .agg(functions.sum("outliers").as("Total Outliers"))


    val outliers = output_data
      .writeStream
      .outputMode("append")
      .format("console")        // can be "orc", "json", "csv", etc.
      /*.option("path", "/home/dimitris/outliers")*/
      .option("truncate", "false")
      .start()

    outliers.awaitTermination()
    spark.streams.active.foreach(_.stop)
  }

  implicit val wr: InfluxWriter[Row] = (o: Row) => {
    val sb = StringBuilder.newBuilder

    var miliseconds = ""
    try {
      miliseconds = "S" * o(2).toString.split("\\.")(1).length
    }

    val pattern = "yyyy-MM-dd HH:mm:ss." + miliseconds
    val date = dateTimeStringToEpoch(o(2).toString, pattern)
    // Query looks like: <measurement>, <tags> <fields> <timestamp RFC3339>
    sb
      .append("Outlier ")
      .append("value=\"")
      .append(o(1))
      .append("\" ")
      .append(date)

    Right(sb.toString())
  }

  def updatePoints(key: Int, values: Iterator[(Int, Data_mcod)], state: GroupState[ListBuffer[(Int, Data_mcod)]]): Iterator[SessionUpdate] = {
    var prevState = state.getOption.getOrElse {
      ListBuffer[(Int, Data_mcod)]()
    }
    var inputList = values.toList

    val inputListBuffer = inputList.to[ListBuffer]
    prevState = prevState ++ inputListBuffer
    val slide = (inputListBuffer.head._2.arrival / slideSize).floor.toInt * slideSize
    if (inputListBuffer.head._2.arrival > (windowSize - slideSize)) {
      prevState.foreach(rec => {
        if (rec._2.arrival < slide - (windowSize - slideSize)) {
          prevState -= rec
        }
      })
    }
    state.update(prevState)
    // create the date/time formatters
    var time_final = System.nanoTime()
    val outliers = new single_query.Pmcod(myQueriesGlobal.head)
      .process(prevState, windowEnd, windowStart)
    time_final = System.nanoTime()
    Iterator(SessionUpdate(outliers._1.outliers.toString, key.toString,outliers._2))
  }

  def updatePointsSlicing(key: Int, values: Iterator[(Int, Data_slicing)], state: GroupState[ListBuffer[(Int, Data_slicing)]]): Iterator[SessionUpdate] = {
    var prevState = state.getOption.getOrElse {
      ListBuffer[(Int, Data_slicing)]()
    }
    var inputList = values.toList
    /*if (inputList.size == 1 && inputList(0)._2.c_point.c_flag == 2) {
      inputList.to[ListBuffer].clear()
    }*/
    val inputListBuffer = inputList.to[ListBuffer]
    prevState = prevState ++ inputListBuffer
    val slide = (inputListBuffer.head._2.arrival / slideSize).floor.toInt * slideSize
    if (inputListBuffer.head._2.arrival > (windowSize - slideSize)) {
      prevState.foreach(rec => {
        if (rec._2.arrival < slide - (windowSize - slideSize)) {
          prevState -= rec
        }
      })
    }
    state.update(prevState)
    // create the date/time formatters
    val outliers = new Slicing(myQueriesGlobal.head)
      .process(prevState, windowEnd, windowStart)

    Iterator(SessionUpdate(outliers._1.outliers.toString, key.toString,outliers._2))
  }

  def updatePointsAdvancedExt(key: Int, values: Iterator[(Int, Data_advanced)], state: GroupState[ListBuffer[(Int, Data_advanced)]]): Iterator[SessionUpdate] = {
    var prevState = state.getOption.getOrElse {
      ListBuffer[(Int, Data_advanced)]()
    }
    var inputList = values.toList
    /*if (inputList.size == 1 && inputList(0)._2.c_point.c_flag == 2) {
      inputList.to[ListBuffer].clear()
    }*/
    val inputListBuffer = inputList.to[ListBuffer]
    prevState = prevState ++ inputListBuffer
    val slide = (inputListBuffer.head._2.arrival / slideSize).floor.toInt * slideSize
    if (inputListBuffer.head._2.arrival > (windowSize - slideSize)) {
      prevState.foreach(rec => {
        if (rec._2.arrival < slide - (windowSize - slideSize)) {
          prevState -= rec
        }
      })
    }
    state.update(prevState)
    // create the date/time formatters
    val outliers = new Advanced_extended(myQueriesGlobal.head)
      .process(prevState, windowEnd, windowStart)

    Iterator(SessionUpdate(outliers._1.outliers.toString, key.toString, outliers._2))
  }

  def dateTimeStringToEpoch(s: String, pattern: String): Long = DateTimeFormatter.ofPattern(pattern).withZone(ZoneOffset.UTC).parse(s, (p: TemporalAccessor) => p.getLong(ChronoField.INSTANT_SECONDS))

  /** User-defined data type representing the input events */
  case class Event(sessionId: String, timestamp: Timestamp)

  case class SessionInfo(
                          numEvents: Int,
                          startTimestampMs: Long,
                          endTimestampMs: Long) {

    /** Duration of the session, between the first and last events */
    def durationMs: Long = endTimestampMs - startTimestampMs
  }

  /**
    * User-defined data type representing the update information returned by mapGroupsWithState.
    *
    * @param outliers Current State
    */
  case class SessionUpdate(
                            outliers: String,
                            partition: String,
                            cpu_time: Long
                          )

  class EventMetric extends StreamingQueryListener{
    override def onQueryStarted(event: QueryStartedEvent): Unit = {
    }

    override def onQueryProgress(event: QueryProgressEvent): Unit = {
      val p = event.progress
      //    println("id : " + p.id)
      println("runId : "  + p.runId)
      //    println("name : " + p.name)
      println("batchid : " + p.batchId)
      println("timestamp : " + p.timestamp)
      println("triggerExecution" + p.durationMs.get("triggerExecution"))
      println(p.eventTime)
      println("inputRowsPerSecond : " + p.inputRowsPerSecond)
      println("numInputRows : " + p.numInputRows)
      println("processedRowsPerSecond : " + p.processedRowsPerSecond)
      println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
    }

    override def onQueryTerminated(event: QueryTerminatedEvent): Unit = {

    }
  }

}