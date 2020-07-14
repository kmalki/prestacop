package drone

import java.sql.Time
import java.time.{LocalDate, Month}
import java.util.concurrent.ThreadLocalRandom
import java.util.{Properties, UUID}
import java.time.format.DateTimeFormatter

import common.{Localisation, Message, ViolationMessage}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.serialization.StringSerializer
import play.api.libs.json.{Json, OWrites}

import scala.util.Random

class Drone() {

  val droneId: String = UUID.randomUUID().toString

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

  def initMessage(): Message = {
    Random.nextInt(5) match {
      //violation
      case 0 | 1 =>
        Message(
          violation = true,
          droneId = droneId,
          violationMessage = Some(ViolationMessage(Random.nextInt(100), UUID.randomUUID().toString)),
          position = randomLocalisation(),
          date = randomDate(),
          time = randomHour(),
          battery = Random.nextInt(101)
        )
      //alert code = 100
      case 2 =>
        Message(
          violation = true,
          droneId = droneId,
          violationMessage = Some(ViolationMessage(100, UUID.randomUUID().toString)),
          position = randomLocalisation(),
          date = randomDate(),
          time = randomHour(),
          battery = Random.nextInt(101)
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
          battery = Random.nextInt(101)
        )
    }
  }

  def sendMessage(): Unit = {

    Stream.from(1).foreach{
      _ => producer.send(
        new ProducerRecord[String, String]("drone-messages", Json.toJson(initMessage()).toString),
        (recordMetaData: RecordMetadata, exception: Exception) => {
          if(exception!=null) {
            exception.printStackTrace()
          }else{
            println(s"common.Message about the sent record: $recordMetaData")
          }
        }
      )
        Thread.sleep(2000)
    }

    producer.close()
  }

}
