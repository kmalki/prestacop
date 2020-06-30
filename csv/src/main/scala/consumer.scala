import java.util

import org.apache.kafka.clients.consumer.KafkaConsumer

import scala.collection.JavaConverters._

object ConsumerExample extends App {

  import java.util.Properties
  import java.io.{BufferedWriter, FileWriter}
  import scala.collection.mutable.ListBuffer
  import au.com.bytecode.opencsv.CSVWriter

  val TOPIC="test"

  val  props = new Properties()
  props.put("bootstrap.servers", "localhost:9092")

  props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("group.id", "something")

  val consumer = new KafkaConsumer[String, String](props)

  consumer.subscribe(util.Collections.singletonList(TOPIC))

  //def rcd(x: Any): Unit ={
    //x = x.asScala

  //}

  val outputFile = new BufferedWriter(new FileWriter("output.csv"))
  val csvWriter = new CSVWriter(outputFile)






  def grosseList (x: Array[String]): Unit ={
    println(x.mkString("\n"))
  }



  def rcd( x: Iterable[org.apache.kafka.clients.consumer.ConsumerRecord[String,String]],y: Array[String]){
    if (x.isEmpty){
      grosseList(y)
      //println("------------------------------------------")
    }
    else{
      println(x.head.value())
      val z = y:+(x.head.value())
      rcd(x.tail,z)
    }
  }

  def consumme(){
      val records = consumer.poll(1000)
      var list = Array[String]()
      rcd(records.asScala,list)
  }

  consumme()

  outputFile.close()

}
