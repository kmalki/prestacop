package csv

import org.apache.kafka.clients.producer._
import play.api.libs.json.{Json, OWrites}
import java.util.{Properties, UUID}

import common.{Localisation, Message, ViolationMessage}
import org.apache.kafka.common.serialization.StringSerializer

import scala.annotation.tailrec
import scala.util.{Random, Try}


class ProducerCsv {

  val  props = new Properties()

  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])

  val producer = new KafkaProducer[String, String](props)


  implicit val localisationJson: OWrites[Localisation] = Json.writes[Localisation]
  implicit val violationJson: OWrites[ViolationMessage] = Json.writes[ViolationMessage]
  implicit val messageJson: OWrites[Message] = Json.writes[Message]

  def tryToInt( s: String ): Option[Int] = Try(s.toInt).toOption

  def ajustHour(x: String,c: String): String = c match {
    case "P" => if (tryToInt(x.substring(0, 4)).isEmpty){"1200"}
                else {(x.substring(0,4).toInt+1200).toString}
    case "A" => x.substring(0,4)
    case "N" => "1200"
  }

  @tailrec
  final def produce(x: Iterator[String]) {
    //verifie si ce n'est pas la dernieres ligne
    if (x.hasNext) {

      //créé une array grace a une ligne du json
      val list = x.next().replaceAll(""",(?!(?:[^"]*"[^"]*")*[^"]*$)""","").split(",")

      //affecte les bonnes valeurs
      val date_msg = list.apply(4) //transformer en format date
      val hour_msg = list.apply(19) // transformer en heure
      val hourstr = if (hour_msg.length > 4) hour_msg.substring(4,5) else "N"

      //cree le message
      val jsMsg = Json.toJson(
        Message(
          violation= true,
        droneId = UUID.randomUUID().toString,
        violationMessage = Some(ViolationMessage(list.apply(5).toInt, UUID.randomUUID().toString)),
        position = Localisation(Random.nextInt(90) + Random.nextFloat(), Random.nextInt(90) + Random.nextFloat()),
        time = ajustHour(hour_msg, hourstr),
        date = date_msg,
        battery = Random.nextInt(100)
        )
      )

      println(jsMsg.toString())

      producer.send(
        new ProducerRecord[String, String]("csv-messages", jsMsg.toString),
        (recordMetaData: RecordMetadata, exception: Exception) => {
          if(exception!=null) {
            exception.printStackTrace()
          }else{
            println(s"common.Message about the sent record: $recordMetaData")
          }
        }
      )

      produce(x)
    }
    else{
      producer.close()
    }
  }
}


