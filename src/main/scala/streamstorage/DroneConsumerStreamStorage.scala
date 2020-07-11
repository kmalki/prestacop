package streamstorage

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{BooleanType, FloatType, IntegerType, StringType, StructType}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

/*case class Localisation(var longitude: Float, var latitude: Float)

case class ViolationMessage(var code: Int, var imageId: String)

case class Message(var violation: Boolean, var droneId: String, var violationMessage: Option[ViolationMessage],
                   var position: Localisation, var date: String, var time: String, var battery: Int)*/

object DroneConsumerStreamStorage {

/*  implicit val localisationReads: Reads[Localisation] = (
    (JsPath \ "longitude").read[Float] and
      (JsPath \ "latitude").read[Float]
    )(Localisation.apply _)


  implicit val violationReads: Reads[ViolationMessage] = (
    (JsPath \ "code").read[Int] and
      (JsPath \ "imageId").read[String]
    )(ViolationMessage.apply _)

  implicit val messageReads: Reads[Message] = (
    (JsPath \ "violation").read[Boolean] and
      (JsPath \ "droneId").read[String] and
      (JsPath \ "violationMessage").readNullable[ViolationMessage] and
      (JsPath \ "position").read[Localisation] and
      (JsPath \ "date").read[String] and
        (JsPath \ "time").read[String] and
      (JsPath \ "battery").read[Int]
    )(Message.apply _)

  def formatToLine(msg: Message): String = {
    if (msg.violation) {
      s"${msg.droneId},${msg.date},${msg.time},${msg.battery}," +
        s"${msg.position.longitude},${msg.position.latitude},${msg.violationMessage.get.code}," +
        s"${msg.violationMessage.get.imageId}"
    } else {
      s"${msg.droneId},${msg.date},${msg.time},${msg.battery}," +
        s"${msg.position.longitude},${msg.position.latitude},,,"
    }
  }*/

  def main(args: Array[String]): Unit = {

    val kafkaParams: Map[String, Object] = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "key.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "messages-drone",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val spark = SparkSession
      .builder()
      .appName("StreamProcessing")
      .master("local[*]")
      .getOrCreate()

    import scala.concurrent.duration._

    val schema = (new StructType()
      .add("droneId",StringType)
      .add("position",(new StructType())
        .add("longitude",FloatType)
        .add("latitude",FloatType)
      )
      .add("date",StringType)
      .add("time",StringType)
      .add("violation",BooleanType)
      .add("violationMessage",(new StructType())
        .add("code",IntegerType)
        .add("imageId",StringType)
      )
      .add("battery",IntegerType)
      )

    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "drone-messages")
      .load()


    df.selectExpr("CAST(value AS STRING)")
      .select(from_json(col("value"), schema).as("data")).select("data.*")
      .select(col("droneId"),
        col("position.*"),
        col("date"),
        col("time"),
        col("violation"),
        col("violationMessage.*"),
        col("battery")
      )
      .writeStream
      .option("format", "append")
      .format("csv")
      .option("header", value = true)
      .option("checkpointLocation", "tmp/msg_stream/sparkcheckpoints")
      .option("path", "drone_msg/")
      .outputMode("append")
      .trigger(Trigger.ProcessingTime(1.minutes))
      .start()
      .awaitTermination()

  }
}
