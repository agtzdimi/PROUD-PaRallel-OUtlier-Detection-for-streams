package outlier_detection

import java.util.Properties

import utils.Helpers.find_gcd
import models._

import scopt.OParser
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.influxdb.InfluxDBConfig
import partitioning.Grid.grid_partitioning
import partitioning.Replication.replication_partitioning
import partitioning.Tree.Tree_partitioning
import utils.Utils.{Evict_before, GroupMetadataAdvanced, GroupMetadataNaive, PrintOutliers, Query, WriteOutliers}
import org.apache.flink.streaming.connectors.influxdb._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema

import scala.collection.mutable.ListBuffer

object Outlier_detection {

  val delimiter = ";"
  val line_delimiter = "&"

  def main(args: Array[String]) {

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
          .validate( x => {
            if (x == "TAO" || x == "STK") success
            else failure("dataset property can only be set as [TAO|STK]")
          })
          .text("Represents the dataset selected for the input. Affects the partitioning technique" +
            "It can either be set to 'TAO' or 'STK'"),
        opt[String]("space")
          .action((x, c) => c.copy(space = x))
          .required()
          .validate( x => {
            if (x == "single" || x == "rk" || x == "rkws") success
            else failure("space property can only be set as [single|rk|rkws]")
          })
          .text("Represents the query space. Possible values are \"single\" for single-query, \"rk\" for multi-query with multiple application parameters and \"rkws\" for multi-query with both multiple application and windowing parameters"),
        opt[String]("algorithm")
          .action((x, c) => c.copy(algorithm = x))
          .required()
          .validate( x => {
            if (x == "naive" || x == "advanced" || x == "advanced_extended" || x == "slicing" ||
              x == "pmcod" || x == "pmcod_net" || x == "amcod" || x == "sop" || x == "psod" || x == "pmcsky") success
            else failure("algorithm property can only be set as [naive|advanced|advanced_extended|slicing|pmcod|pmcod_net|amcod|sop|psod|pmcsky]")
          })
          .text("Represents the outlier detection algorithm. Possible values for single-query space are: \"naive\", \"advanced\", \"advanced_extended\", \"slicing\", \"pmcod\" and \"pmcod_net\". Possible values for multi-query space are \"amcod\", \"sop\", \"psod\" and \"pmcsky\""),
        opt[String]("checkPointDir")
          .action((x, c) => c.copy(dataset = x))
          .required()
          .validate( x => {
            if (x.startsWith("/")) success
            else failure("The Checkpoint Directory should be a full path directory starting with '/'")
          })
          .text("checkPointDir contains the full path directory to store the spark checkpoint of the application"),
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
          .validate( x => {
            if (x == "replication" || x == "grid" || x == "tree") success
            else failure("Represents the partitioning technique chosen for the job. \"replication\" partitioning is mandatory for \"naive\" and \"advanced\" algorithms whilst \"grid\" and \"tree\" partitioning is available for every other algorithm. \"grid\" technique needs pre-recorded data on the dataset's distribution. \"tree\" technique needs a file containing data points from the dataset in order to initialize the VP-tree")
          })
          .text("partitioning can either be set to 'replication'/'grid'/'tree'"),
        opt[Int]("tree_init")
          .action((x, c) => c.copy(tree_init = x))
          .text("(Optional) Represents the number of data points to be read for initialization by the \"tree\" partitioning technique. Default value is 10000"),
      )
    }

    var arguments: Config = Config()

    // OParser.parse returns Option[Config]
    OParser.parse(parser, args, Config()) match {
      case Some(config) =>
        arguments = Config(config.DEBUG,config.dataset,config.space,config.checkPointDir,config.algorithm,config.W,config.S,config.k,config.R,config.partitioning,config.tree_init)
      case _ =>
      // arguments are bad, error message will have been displayed
    }
    
    //Hardcoded parameter for partitioning and windowing
    val partitions = 16
    //Input file
    val file_input = System.getenv("JOB_INPUT") //File input
    //Input topic
    val kafka_topic = System.getenv("KAFKA_TOPIC") //File input

    //Pre-process parameters for initializations
    val common_W = if (arguments.W.length > 1) arguments.W.max else arguments.W.head
    val common_S = if (arguments.S.length > 1) find_gcd(arguments.S) else arguments.S.head
    val common_R = if (arguments.R.length > 1) arguments.R.max else arguments.R.head
    //Variable to allow late data points within the specific time be ingested by the job
    val allowed_lateness = common_S

    //Create query/queries
    val myQueries = new ListBuffer[Query]()
    for (w <- arguments.W)
      for (s <- arguments.S)
        for (r <- arguments.R)
          for (k <- arguments.k)
            myQueries += Query(r, k, w, s, 0)


    //Create the tree for the specified partitioning type
    val myTree = if (arguments.partitioning == "tree") {
      val tree_input = s"$file_input/$arguments.dataset/tree_input.txt"
      new Tree_partitioning(arguments.tree_init, partitions, tree_input)
    }
    else null

    //InfluxDB
    val influxdb_host = System.getenv("INFLUXDB_HOST")
    val influxdb_user = System.getenv("INFLUXDB_USER")
    val influxdb_pass = System.getenv("INFLUXDB_PASSWORD")
    val influxdb_db = System.getenv("INFLUXDB_DB")
    val influx_conf: InfluxDBConfig =
      InfluxDBConfig.builder(influxdb_host, influxdb_user, influxdb_pass, influxdb_db).createDatabase(true)
        .build()
    //Kafka brokers
    val kafka_brokers = System.getenv("KAFKA_BROKERS")
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", kafka_brokers)

    //Start context
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(partitions)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //Start reading from source
    //Either kafka or input file for debug
    val data: DataStream[Data_basis] = if (arguments.DEBUG) {
      val myInput = s"$file_input/$arguments.arguments.dataset/input_20k.txt"
      env
        .readTextFile(myInput)
        .map { record =>
          val splitLine = record.split(line_delimiter)
          val id = splitLine(0).toInt
          val value = splitLine(1).split(delimiter).map(_.toDouble).to[ListBuffer]
          val timestamp = id.toLong
          new Data_basis(id, value, timestamp, 0)
        }
    } else {
      env.addSource(new FlinkKafkaConsumer[ObjectNode](kafka_topic, new JSONKeyValueDeserializationSchema(false), properties))
        .map{record =>
          val id = record.get("value").get("id").asInt
          val timestamp = record.get("value").get("timestamp").asLong
          val value_iter = record.get("value").get("value").elements
          val value = ListBuffer[Double]()
          while(value_iter.hasNext) {
            value += value_iter.next().asDouble()
          }
          new Data_basis(id, value, timestamp, 0)
        }
    }

    //Start partitioning
    val partitioned_data = arguments.partitioning match {
      case "replication" =>
        data
          .flatMap(record => replication_partitioning(partitions, record))
      case "grid" =>
        data
          .flatMap(record => grid_partitioning(partitions, record, common_R, arguments.dataset))
      case "tree" =>
        data
          .flatMap(record => myTree.tree_partitioning(partitions, record, common_R))
    }

    //Timestamp the data
    val timestamped_data = partitioned_data
      .assignAscendingTimestamps(_._2.arrival)

    //Start algorithm
    //Start by mapping the basic data point to the algorithm's respective class
    //Key the data by partition and window them
    //Call the algorithm process
    val output_data = arguments.space match {
      case "single" =>
        arguments.algorithm match {
          case "naive" =>
            val firstWindow: DataStream[Data_naive] = timestamped_data
              .map(record => (record._1, new Data_naive(record._2)))
              .keyBy(_._1)
              .timeWindow(Time.milliseconds(common_W), Time.milliseconds(common_S))
              .allowedLateness(Time.milliseconds(allowed_lateness))
              .evictor(new Evict_before(common_S))
              .process(new single_query.Naive(myQueries.head))
            firstWindow
              .keyBy(_.id % partitions)
              .timeWindow(Time.milliseconds(common_S))
              .process(new GroupMetadataNaive(myQueries.head))
          case "advanced" =>
            val firstWindow: DataStream[Data_advanced] = timestamped_data
              .map(record => (record._1, new Data_advanced(record._2)))
              .keyBy(_._1)
              .timeWindow(Time.milliseconds(common_W), Time.milliseconds(common_S))
              .allowedLateness(Time.milliseconds(allowed_lateness))
              .evictor(new Evict_before(common_S))
              .process(new single_query.Advanced(myQueries.head))
            firstWindow
              .keyBy(_.id % partitions)
              .timeWindow(Time.milliseconds(common_S))
              .process(new GroupMetadataAdvanced(myQueries.head))
          case "advanced_extended" =>
            timestamped_data
              .map(record => (record._1, new Data_advanced(record._2)))
              .keyBy(_._1)
              .timeWindow(Time.milliseconds(common_W), Time.milliseconds(common_S))
              .allowedLateness(Time.milliseconds(allowed_lateness))
              .process(new single_query.Advanced_extended(myQueries.head))
          case "slicing" =>
            timestamped_data
              .map(record => (record._1, new Data_slicing(record._2)))
              .keyBy(_._1)
              .timeWindow(Time.milliseconds(common_W), Time.milliseconds(common_S))
              .allowedLateness(Time.milliseconds(allowed_lateness))
              .process(new single_query.Slicing(myQueries.head))
          case "pmcod" =>
            timestamped_data
              .map(record => (record._1, new Data_mcod(record._2)))
              .keyBy(_._1)
              .timeWindow(Time.milliseconds(common_W), Time.milliseconds(common_S))
              .allowedLateness(Time.milliseconds(allowed_lateness))
              .process(new single_query.Pmcod(myQueries.head))
          case "pmcod_net" =>
            timestamped_data
              .map(record => (record._1, new Data_mcod(record._2)))
              .keyBy(_._1)
              .timeWindow(Time.milliseconds(common_W), Time.milliseconds(common_S))
              .allowedLateness(Time.milliseconds(allowed_lateness))
              .process(new single_query.Pmcod_net(myQueries.head))
        }
      case "rk" =>
        arguments.algorithm match {
          case "amcod" =>
            timestamped_data
              .map(record => (record._1, new Data_amcod(record._2)))
              .keyBy(_._1)
              .timeWindow(Time.milliseconds(common_W), Time.milliseconds(common_S))
              .allowedLateness(Time.milliseconds(allowed_lateness))
              .process(new rk_query.Amcod(myQueries))
          case "sop" =>
            timestamped_data
              .map(record => (record._1, new Data_lsky(record._2)))
              .keyBy(_._1)
              .timeWindow(Time.milliseconds(common_W), Time.milliseconds(common_S))
              .allowedLateness(Time.milliseconds(allowed_lateness))
              .process(new rk_query.Sop(myQueries))
          case "psod" =>
            timestamped_data
              .map(record => (record._1, new Data_lsky(record._2)))
              .keyBy(_._1)
              .timeWindow(Time.milliseconds(common_W), Time.milliseconds(common_S))
              .allowedLateness(Time.milliseconds(allowed_lateness))
              .process(new rk_query.Psod(myQueries))
          case "pmcsky" =>
            timestamped_data
              .map(record => (record._1, new Data_mcsky(record._2)))
              .keyBy(_._1)
              .timeWindow(Time.milliseconds(common_W), Time.milliseconds(common_S))
              .allowedLateness(Time.milliseconds(allowed_lateness))
              .process(new rk_query.PmcSky(myQueries))
        }
      case "rkws" =>
        arguments.algorithm match {
          case "sop" =>
            timestamped_data
              .map(record => (record._1, new Data_lsky(record._2)))
              .keyBy(_._1)
              .timeWindow(Time.milliseconds(common_W), Time.milliseconds(common_S))
              .allowedLateness(Time.milliseconds(allowed_lateness))
              .process(new rkws_query.Sop(myQueries, common_S))
          case "psod" =>
            timestamped_data
              .map(record => (record._1, new Data_lsky(record._2)))
              .keyBy(_._1)
              .timeWindow(Time.milliseconds(common_W), Time.milliseconds(common_S))
              .allowedLateness(Time.milliseconds(allowed_lateness))
              .process(new rkws_query.Psod(myQueries, common_S))
          case "pmcsky" =>
            timestamped_data
              .map(record => (record._1, new Data_mcsky(record._2)))
              .keyBy(_._1)
              .timeWindow(Time.milliseconds(common_W), Time.milliseconds(common_S))
              .allowedLateness(Time.milliseconds(allowed_lateness))
              .process(new rkws_query.PmcSky(myQueries, common_S))
        }
    }

    //Start writing output
    if(arguments.DEBUG){
    output_data
      .keyBy(_._1)
      .timeWindow(Time.milliseconds(common_S))
      .process(new PrintOutliers)
      .print
    } else {
      output_data
        .keyBy(_._1)
        .timeWindow(Time.milliseconds(common_S))
        .process(new WriteOutliers)
        .addSink(new InfluxDBSink(influx_conf))
    }
    env.execute("Distance-based outlier detection with Flink")
  }

}
