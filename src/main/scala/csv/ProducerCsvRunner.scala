package csv

import java.io.File

import scala.annotation.tailrec
import scala.io.Source

object ProducerCsvRunner {
  def main(args: Array[String]): Unit = {

    val listFile = new File("csv").listFiles.filter(_.isFile).toList

    def runProducer(x: File): Any = {
      val input = Source.fromFile(x).getLines()
      input.next()
      new ProducerCsv().produce(input)
    }

    @tailrec
    def matchCsv(x: List[File]): Any = x match {
      case head::tail => runProducer(head);matchCsv(tail)
      case head::Nil => runProducer(head)
      case Nil => println("fin")
      case _ => println("error")
    }

    matchCsv(listFile)


  }
}

