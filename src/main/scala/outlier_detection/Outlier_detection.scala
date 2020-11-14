package outlier_detection

import java.time.temporal.TemporalAccessor

import com.github.fsanaulla.chronicler.core.alias.ErrorOr
import com.github.fsanaulla.chronicler.core.model.{InfluxCredentials, InfluxWriter}
import com.github.fsanaulla.chronicler.spark.core.CallbackHandler
import com.github.fsanaulla.chronicler.spark.structured.streaming._
import com.github.fsanaulla.chronicler.urlhttp.shared.InfluxConfig
import models.{Data_basis, Data_naive}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{col, collect_set, window}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.streaming.{Seconds, Time}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.{SparkConf, SparkContext}
import partitioning.Tree.Tree_partitioning
import scopt.OParser
import utils.Helpers.find_gcd
import utils.Utils.Query
import partitioning.Replication.replication_partitioning
import partitioning.Grid.grid_partitioning

import scala.collection.mutable.ListBuffer

object Outlier_detection {

  import java.time.ZoneOffset
  import java.time.format.DateTimeFormatter
  import java.time.temporal.ChronoField
  val delimiter = ";"

  implicit val wr: InfluxWriter[Row] = new InfluxWriter[Row] {
    override def write(o: Row): ErrorOr[String] = {
      val sb = StringBuilder.newBuilder

      val date = dateTimeStringToEpoch(o(2).toString(), "yyyy-MM-dd HH:mm:ss.SSS")
      // Query looks like: <measurement>, <tags> <fields> <timestamp RFC3339>
      sb
        .append("Outlier ")
        .append("value=\"")
        .append(o(1))
        .append("\" ")
        .append(date)

      Right(sb.toString())
    }
  }

  def dateTimeStringToEpoch(s: String, pattern: String): Long = DateTimeFormatter.ofPattern(pattern).withZone(ZoneOffset.UTC).parse(s, (p: TemporalAccessor) => p.getLong(ChronoField.INSTANT_SECONDS))

  def main(args: Array[String]) {

    val conf = new SparkConf().setMaster("local[*]")
      .setAppName("Data Mining Project").setSparkHome(".")
    conf.set("spark.driver.memory", "14g")
    conf.set("spark.hadoop.validateOutputSpecs", "false")
    conf.set("spark.executor.instances", "1")
    conf.set("spark.executor.cores", "8")
    conf.set("spark.executor.memory", "3g")
    conf.set("spark.cores.max", "8")
    conf.set("spark.eventLog.enabled", "true")
    conf.set("spark.eventLog.dir", "spark-logs")
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)
    val sc = new SparkContext(conf)

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
            if (x == "TAO" || x == "STK") success
            else failure("dataset property can only be set as [TAO|STK]")
          })
          .text("Represents the dataset selected for the input. Affects the partitioning technique" +
            "It can either be set to 'TAO' or 'STK'"),
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

    //Kafka brokers
    val kafka_brokers = System.getenv("KAFKA_BROKERS")

    val spark = SparkSession
      .builder
      .appName("PROUD Spark Streaming")
      .master("local[" + partitions + "]")
      .config("spark.sql.streaming.checkpointLocation", "/home/dimitris/checkPointDir")
      .getOrCreate()

    println(arguments.DEBUG)
    println(kafka_brokers)
    println(kafka_topic)

    val data = if (arguments.DEBUG) {
      val myInput = s"$file_input/$arguments.arguments.dataset/input_20k.txt"
      val debugFile = spark.readStream.text(myInput)
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
    val query = data
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "CAST(timestamp AS STRING)")
      .writeStream
      .format("console")
      .start()

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

    val saveToInflux = data
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "CAST(timestamp AS STRING)")
      .writeStream
      .saveToInfluxDB(influxdb_db, handler)
      .start()

    import spark.implicits._

    var key: Int =0
    val windowedData = data
      .withColumn("timestamp", ($"timestamp").cast(TimestampType))
      .withWatermark("timestamp",s"$common_W seconds")
      .groupBy(
        window($"timestamp", s"$common_W seconds", s"$common_S seconds"),$"key",$"value",$"timestamp"
      )
      .agg($"key", $"value".cast("String").as("FinalValue"), $"timestamp")
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "CAST(timestamp AS STRING)","window")
      .writeStream
      .outputMode("complete")
      .trigger(Trigger.ProcessingTime(10))
      .foreachBatch {
        (batchDs: Dataset[Row], batchId: Long) =>
          val data2 = batchDs
            .map( (record) => {
              import models.ExtraDataFrameOperations.implicits._
              val windowTimes = record.get(2).toString()
              key += 1
              val date = dateTimeStringToEpoch(windowTimes, "yyyy-MM-dd HH:mm:ss.SSS")
              new Data_basis(key,ListBuffer(record.get(1).toString().split(",").map(_.toDouble): _ *),date, 0)
            })

          //Start partitioning
          val partitioned_data = arguments.partitioning match {
            case "replication" =>
              data2
                .flatMap(record => replication_partitioning(partitions, record))
            case "grid" =>
              data2
                .flatMap(record => grid_partitioning(partitions, record, common_R, arguments.dataset))
            case "tree" =>
              data2
                .flatMap(record => myTree.tree_partitioning(partitions, record, common_R))
          }

          val naiveQ = new single_query.Naive(myQueries.head)
          naiveQ.process(partitioned_data.map(record => (record._1, new Data_naive(record._2))), 5, spark)
      }
      .start()


    query.awaitTermination()
    windowedData.awaitTermination()
    saveToInflux.awaitTermination()
    //Timestamp the data

    //Start algorithm
    //Start by mapping the basic data point to the algorithm's respective class
    //Key the data by partition and window them
    //Call the algorithm process
 /*   val output_data = arguments.space match {
      case "single" =>
        arguments.algorithm match {
          case "naive" =>
            val firstWindow: DStream[Data_naive] = partitioned_data
              .map(record => (record._1, new Data_naive(record._2)))
              .groupBy(
                window($"timestamp", s"$common_W seconds", s"$common_S seconds")
              )

              .evictor(new Evict_before(common_S))
              .process(new single_query.Naive(myQueries.head))
            firstWindow
              .keyBy(_.id % partitions)
              .timeWindow(Time.milliseconds(common_S))
              .process(new GroupMetadataNaive(myQueries.head))
          case "advanced" =>
            val firstWindow: DataStream[Data_advanced] = partitioned_data
              .map(record => (record._1, new Data_advanced(record._2)))
              .groupByKey(record => record._1)
              .timeWindow(Time.milliseconds(common_W), Time.milliseconds(common_S))
              .allowedLateness(Time.milliseconds(allowed_lateness))
              .evictor(new Evict_before(common_S))
              .process(new single_query.Advanced(myQueries.head))
            firstWindow
              .keyBy(_.id % partitions)
              .timeWindow(Time.milliseconds(common_S))
              .process(new GroupMetadataAdvanced(myQueries.head))
          case "advanced_extended" =>
            partitioned_data
              .map(record => (record._1, new Data_advanced(record._2)))
              .keyBy(_._1)
              .timeWindow(Time.milliseconds(common_W), Time.milliseconds(common_S))
              .allowedLateness(Time.milliseconds(allowed_lateness))
              .process(new single_query.Advanced_extended(myQueries.head))
          case "slicing" =>
            partitioned_data
              .map(record => (record._1, new Data_slicing(record._2)))
              .keyBy(_._1)
              .timeWindow(Time.milliseconds(common_W), Time.milliseconds(common_S))
              .allowedLateness(Time.milliseconds(allowed_lateness))
              .process(new single_query.Slicing(myQueries.head))
          case "pmcod" =>
            partitioned_data
              .map(record => (record._1, new Data_mcod(record._2)))
              .keyBy(_._1)
              .timeWindow(Time.milliseconds(common_W), Time.milliseconds(common_S))
              .allowedLateness(Time.milliseconds(allowed_lateness))
              .process(new single_query.Pmcod(myQueries.head))
          case "pmcod_net" =>
            partitioned_data
              .map(record => (record._1, new Data_mcod(record._2)))
              .keyBy(_._1)
              .timeWindow(Time.milliseconds(common_W), Time.milliseconds(common_S))
              .allowedLateness(Time.milliseconds(allowed_lateness))
              .process(new single_query.Pmcod_net(myQueries.head))
        }
      case "rk" =>
        arguments.algorithm match {
          case "amcod" =>
            partitioned_data
              .map(record => (record._1, new Data_amcod(record._2)))
              .keyBy(_._1)
              .timeWindow(Time.milliseconds(common_W), Time.milliseconds(common_S))
              .allowedLateness(Time.milliseconds(allowed_lateness))
              .process(new rk_query.Amcod(myQueries))
          case "sop" =>
            partitioned_data
              .map(record => (record._1, new Data_lsky(record._2)))
              .keyBy(_._1)
              .timeWindow(Time.milliseconds(common_W), Time.milliseconds(common_S))
              .allowedLateness(Time.milliseconds(allowed_lateness))
              .process(new rk_query.Sop(myQueries))
          case "psod" =>
            partitioned_data
              .map(record => (record._1, new Data_lsky(record._2)))
              .keyBy(_._1)
              .timeWindow(Time.milliseconds(common_W), Time.milliseconds(common_S))
              .allowedLateness(Time.milliseconds(allowed_lateness))
              .process(new rk_query.Psod(myQueries))
          case "pmcsky" =>
            partitioned_data
              .map(record => (record._1, new Data_mcsky(record._2)))
              .keyBy(_._1)
              .timeWindow(Time.milliseconds(common_W), Time.milliseconds(common_S))
              .allowedLateness(Time.milliseconds(allowed_lateness))
              .process(new rk_query.PmcSky(myQueries))
        }
      case "rkws" =>
        arguments.algorithm match {
          case "sop" =>
            partitioned_data
              .map(record => (record._1, new Data_lsky(record._2)))
              .keyBy(_._1)
              .timeWindow(Time.milliseconds(common_W), Time.milliseconds(common_S))
              .allowedLateness(Time.milliseconds(allowed_lateness))
              .process(new rkws_query.Sop(myQueries, common_S))
          case "psod" =>
            partitioned_data
              .map(record => (record._1, new Data_lsky(record._2)))
              .keyBy(_._1)
              .timeWindow(Time.milliseconds(common_W), Time.milliseconds(common_S))
              .allowedLateness(Time.milliseconds(allowed_lateness))
              .process(new rkws_query.Psod(myQueries, common_S))
          case "pmcsky" =>
            partitioned_data
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
    env.execute("Distance-based outlier detection with Flink")*/
    spark.stop()
  }

}
