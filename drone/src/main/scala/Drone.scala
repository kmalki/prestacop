import java.util.{Calendar, Date, Properties, UUID}

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.serialization.StringSerializer

import scala.util.Random

case class Position(var longitude: Float, var latitude: Float)

case class ViolationMessage(var code: String, var imageId: String)
case class Message(var violation: Boolean, var droneId: String, var violationMessage: ViolationMessage,
                   var position: Position, var time: Date, var battery: Int)

class Drone() {

  var droneId: String = UUID.randomUUID().toString

  var position: Position = Position(Random.nextInt(90) + Random.nextFloat(), Random.nextInt(90) + Random.nextFloat())

  var time: Date = Calendar.getInstance.getTime

  var battery: Int = 100

  val props: Properties = new Properties()

  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])

  val producer: KafkaProducer[String, String] = new KafkaProducer[String, String](props)

  def sendMessage(): Unit = {
    time = Calendar.getInstance.getTime
    val record = new ProducerRecord[String, String]("messages",
      Message(
        violation = false,
        droneId = droneId,
        violationMessage = null,
        position = position,
        time = time,
        battery = battery)
        .toString
    )

    producer.send(record, (recordMetaData: RecordMetadata, exception: Exception) => {
      if(exception!=null) {
        exception.printStackTrace()
      }else{
        println(s"Message about the sent record: $recordMetaData")
      }
    }
    )

    sendMessage()
  }

}
