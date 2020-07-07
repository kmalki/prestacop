object ProducerExample extends App {

  import java.util.Properties

  import org.apache.kafka.clients.producer._

  val  props = new Properties()
  props.put("bootstrap.servers", "localhost:9092")

  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

  val producer = new KafkaProducer[String, String](props)

  val TOPIC="test"

  /*val input = io.Source.fromFile("Parking_Violations_Issued_-_Fiscal_Year_2015.csv").getLines()

  def produce( x: Iterator[String]){
    if (x.hasNext == true){
      val record = new ProducerRecord(TOPIC, "key", x.next())
      producer.send(record)
      produce(x)
    }
    else{
      producer.close()
    }
  }
  produce(input)*/

}
