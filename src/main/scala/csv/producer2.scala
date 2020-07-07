import org.apache.kafka.clients.producer._
import play.api.libs.json.{Json, OWrites}
import java.util.{Date, Properties, UUID}
import scala.util.Random

case class Localisation(var longitude: Float, var latitude: Float)
case class ViolationMessage(var code: Int, var imageId: String)
case class Message(var violation: Boolean, var droneId: String, var violationMessage: Option[ViolationMessage],
                   var position: Localisation, var Hour: String ,var time: Date, var battery: Int)

class Producertwo {

  val  props = new Properties()

  props.put("bootstrap.servers", "localhost:9092")
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

  val producer = new KafkaProducer[String, String](props)


  implicit val localisationJson: OWrites[Localisation] = Json.writes[Localisation]
  implicit val violationJson: OWrites[ViolationMessage] = Json.writes[ViolationMessage]
  implicit val messageJson: OWrites[Message] = Json.writes[Message]

  val format = new java.text.SimpleDateFormat("MM/dd/yyyy")


  def ajustHour(x: String,c: String): String = c match {
    case "P" => (x.substring(0,4).toInt+1200).toString
    case "A" => x.substring(0,4)
    case "N" => "1200"
  }

  def produce(x: Iterator[String]) {
    //verifie si ce n'est pas la dernieres ligne
    if (x.hasNext) {

      //créé une array grace a une ligne du json
      val list = x.next().replaceAll(""",(?!(?:[^"]*"[^"]*")*[^"]*$)""","").split(",")

      //affecte les bonnes valeurs
      val dateo = list.apply(4) //transformer en format date
      val hour = list.apply(19) // transformer en heure
      val hourstr = if (hour.length > 4) hour.substring(4,5) else "N"

      //cree le message
      val jsMsg = Json.toJson(
        Message(
          violation= true,
        droneId = UUID.randomUUID().toString,
        violationMessage = Some(ViolationMessage(list.apply(5).toInt, UUID.randomUUID().toString)),
        position = Localisation(Random.nextInt(90) + Random.nextFloat(), Random.nextInt(90) + Random.nextFloat()),
        Hour = ajustHour(hour,hourstr),
        time = format.parse(dateo),
        battery = 100 ))

      println(jsMsg)
      print(list.apply(0))
      val record = new ProducerRecord[String, String]("test", jsMsg.toString)
      //val record = new ProducerRecord(TOPIC, x.next())
      produce(x)
    }
    else{
      producer.close()
    }
  }
}


