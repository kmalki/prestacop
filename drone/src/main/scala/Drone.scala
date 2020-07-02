import java.util.{Calendar, Date, Properties, UUID}

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.serialization.StringSerializer
import play.api.libs.json.{Format, JsNumber, JsObject, JsResult, JsString, JsValue, Json, OWrites}

import scala.util.Random

case class Localisation(var longitude: Float, var latitude: Float)

case class ViolationMessage(var code: Int, var imageId: String)

case class Message(var violation: Boolean, var droneId: String, var violationMessage: Option[ViolationMessage],
                   var position: Localisation, var time: Date, var battery: Int)

class Drone() {

  var droneId: String = UUID.randomUUID().toString

  var battery: Int = 100

  val props: Properties = new Properties()

  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])

  val producer: KafkaProducer[String, String] = new KafkaProducer[String, String](props)

  implicit val localisationJson: OWrites[Localisation] = Json.writes[Localisation]
  implicit val violationJson: OWrites[ViolationMessage] = Json.writes[ViolationMessage]
  implicit val messageJson: OWrites[Message] = Json.writes[Message]

  def sendMessage(): Unit = {

    val time: Date = Calendar.getInstance.getTime

    val position = Localisation(Random.nextInt(90) + Random.nextFloat(), Random.nextInt(90) + Random.nextFloat())

    val choice = Random.nextInt(5)

    val message = choice match {
        //violation
      case 0 =>
        Message(
          violation = true,
          droneId = droneId,
          violationMessage = Some(ViolationMessage(Random.nextInt(100), UUID.randomUUID().toString)),
          position = position,
          time = time,
          battery = battery
        )
        //alert code = 100
      case 1 =>
        Message(
          violation = true,
          droneId = droneId,
          violationMessage = Some(ViolationMessage(100, UUID.randomUUID().toString)),
          position = position,
          time = time,
          battery = battery
        )
      //regular message
      case _ =>
        Message(
          violation = false,
          droneId = droneId,
          violationMessage = None,
          position = position,
          time = time,
          battery = battery
        )
    }

    println("code"+choice)

    val jsMsg = Json.toJson(message)

    val record = new ProducerRecord[String, String]("messages",
      jsMsg.toString
    )

    producer.send(record, (recordMetaData: RecordMetadata, exception: Exception) => {
      if(exception!=null) {
        exception.printStackTrace()
      }else{
        println(s"Message about the sent record: $recordMetaData")
      }
    }
    )

    Thread.sleep(5000)

    sendMessage()
  }

}
