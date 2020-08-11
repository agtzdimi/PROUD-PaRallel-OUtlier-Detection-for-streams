package custom_source

import scopt.OParser
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

object Custom_source {

  def main(args: Array[String]): Unit = {

    case class Config(
                       DEBUG: Boolean = false,
                       dataset: String = "",
                       checkPointDir: String = "",
                       kwargs: Map[String, String] = Map())

    val builder = OParser.builder[Config]

    val parser = {
      import builder._
      OParser.sequence(
        programName("PROUD Spark Streaming"),
        head("scopt", "4.x"),
        opt[Boolean]("DEBUG")
          .action((x, c) => c.copy(DEBUG = x))
          .text("DEBUG is a Boolean property"),
        opt[String]("dataset")
          .action((x, c) => c.copy(dataset = x))
          .validate( x => {
            if (x == "TAO" || x == "STK") success
            else failure("dataset property can only be set as [TAO|STK]")
          })
          .text("Dataset property contains the type of dataset that will be used in the PROUD application" +
            "It can either be set to 'TAO' or 'STK'"),
        opt[String]("checkPointDir")
          .action((x, c) => c.copy(dataset = x))
          .validate( x => {
            if (x.startsWith("/")) success
            else failure("The Checkpoint Directory should be a full path directory starting with '/'")
          })
          .text("checkPointDir contains the full path directory to store the spark checkpoint of the application"),

      )
    }

    var arguments: Config = Config()

    // OParser.parse returns Option[Config]
     OParser.parse(parser, args, Config()) match {
      case Some(config) =>
        arguments = Config(config.DEBUG,config.dataset,config.checkPointDir)
      case _ =>
      // arguments are bad, error message will have been displayed
    }

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> System.getenv("KAFKA_BROKERS"),
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    // Create the context with a 1 second batch size
    val ssc = new StreamingContext("local[*]", "PROUD Spark Streaming", Seconds(1))

    val topics = List(System.getenv("KAFKA_TOPIC"))
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    val jsonProducer = new Json_source(arguments.dataset)


    val data = stream.map(record => (record.key, record.value))

    data.map(v => {
      val record = jsonProducer.get_next()
      println(record)
    })
    // Kick it off
    ssc.checkpoint(arguments.checkPointDir)
    ssc.start()
    ssc.awaitTermination()
  }

}
