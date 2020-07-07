package streamstorage

import java.util.Date

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.{KafkaUtils, LocationStrategies}
import play.api.libs.json.{JsError, JsPath, JsSuccess, Json, Reads}

case class Localisation(var longitude: Float, var latitude: Float)

case class ViolationMessage(var code: Int, var imageId: String)

case class Message(var violation: Boolean, var droneId: String, var violationMessage: Option[ViolationMessage],
                   var position: Localisation, var date: String, var time: String, var battery: Int)

object Main {
  def main(args: Array[String]): Unit = {

    implicit val localisationJson: Reads[Localisation] = Json.reads[Localisation]
    implicit val violationJson: Reads[ViolationMessage] = Json.reads[ViolationMessage]
    implicit val messageJson: Reads[Message] = Json.reads[Message]

    val sparkConf: SparkConf = new SparkConf().setAppName("streamToStorage").setMaster("local[2]")
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(5))

    val kafkaParams: Map[String, Object] = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "key.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "messages-drone",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics: Array[String] = Array("messages")

    val stream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      ssc, LocationStrategies.PreferConsistent, Subscribe[String, String](topics, kafkaParams)
    )

    stream
      .map{
        record => {
/*          Json.fromJson[Message](Json.parse(record.value)) match {
            case JsSuccess(m: Message, path: JsPath) => m
            case e @ JsError(_) =>
          }*/
          Json.parse(record.value).as[Message]
          record.value
      }
      }.print()

    ssc.start()

    ssc.awaitTermination()  }
}
