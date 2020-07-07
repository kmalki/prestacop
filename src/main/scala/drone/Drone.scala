package drone

import java.sql.Time
import java.time.{LocalDate, Month}
import java.util.concurrent.ThreadLocalRandom
import java.util.{Properties, UUID}
import java.time.format.DateTimeFormatter

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.serialization.StringSerializer
import play.api.libs.json.{Json, OWrites}

import scala.util.Random

case class Localisation(var longitude: Float, var latitude: Float)

case class ViolationMessage(var code: Int, var imageId: String)

case class Message(var violation: Boolean, var droneId: String, var violationMessage: Option[ViolationMessage],
                   var position: Localisation, var date: String, var time: String, var battery: Int)

class Drone() {

  val droneId: String = UUID.randomUUID().toString

  var battery: Int = 100

  val pattern: DateTimeFormatter = DateTimeFormatter.ofPattern("MM/dd/yyyy")

  val props: Properties = new Properties()

  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])

  val producer: KafkaProducer[String, String] = new KafkaProducer[String, String](props)

  implicit val localisationJson: OWrites[Localisation] = Json.writes[Localisation]
  implicit val violationJson: OWrites[ViolationMessage] = Json.writes[ViolationMessage]
  implicit val messageJson: OWrites[Message] = Json.writes[Message]

  def randomDate(): String = {
    pattern.format(LocalDate.ofEpochDay(
      ThreadLocalRandom.current.nextLong(LocalDate.of(2020, Month.JANUARY, 1).toEpochDay,
        LocalDate.now().toEpochDay)))
  }

  def randomHour(): String = {
    val time = new Time(Random.nextInt(24 * 60 * 60 * 1000).asInstanceOf[Long]).toString
    time.split(":").dropRight(1).mkString("")
  }

  def randomLocalisation(): Localisation = {
    Localisation(Random.nextInt(90) + Random.nextFloat(), Random.nextInt(90) + Random.nextFloat())
  }

  def updateBattery(): Unit = {
    battery = battery - {
      Random.nextInt(3) match {
      case 0 => 1
      case _ => 0
      }
    }
  }

  def sendMessage(): Unit = {

    val choice = Random.nextInt(5)

    val message = choice match {
        //violation
      case 0 =>
        Message(
          violation = true,
          droneId = droneId,
          violationMessage = Some(ViolationMessage(Random.nextInt(100), UUID.randomUUID().toString)),
          position = randomLocalisation(),
          date = randomDate(),
          time = randomHour(),
          battery = battery
        )
        //alert code = 100
      case 1 =>
        Message(
          violation = true,
          droneId = droneId,
          violationMessage = Some(ViolationMessage(100, UUID.randomUUID().toString)),
          position = randomLocalisation(),
          date = randomDate(),
          time = randomHour(),
          battery = battery
        )
      //regular message
      case _ =>
        Message(
          violation = false,
          droneId = droneId,
          violationMessage = None,
          position = randomLocalisation(),
          date = randomDate(),
          time = randomHour(),
          battery = battery
        )
    }

    val record = new ProducerRecord[String, String]("messages",
      Json.toJson(message).toString
    )

    producer.send(record, (recordMetaData: RecordMetadata, exception: Exception) => {
      if(exception!=null) {
        exception.printStackTrace()
      }else{
        println(s"Message about the sent record: $recordMetaData")
      }
    }
    )

    updateBattery()

    Thread.sleep(1000)

    sendMessage()

    producer.close()
  }

}
