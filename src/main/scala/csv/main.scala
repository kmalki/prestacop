package csv
import java.io.BufferedReader
import java.io.FileReader

import scala.annotation.tailrec
import scala.io.Source

object main {
  def main(args: Array[String]): Unit = {

    val listFile = List("/home/hedi/IdeaProjects/drone/csv/project/Parking_Violations_Issued_-_Fiscal_Year_2014__August_2013___June_2014_.csv","/home/hedi/IdeaProjects/drone/csv/project/Parking_Violations_Issued_-_Fiscal_Year_2015.csv","/home/hedi/IdeaProjects/drone/csv/project/Parking_Violations_Issued_-_Fiscal_Year_2016.csv","/home/hedi/IdeaProjects/drone/csv/project/Parking_Violations_Issued_-_Fiscal_Year_2017.csv")

    def runProducer(x: String): Any = {
      val input = Source.fromFile(x).getLines()
      input.next()
      val producer = new Producertwo
      producer.produce(input)

    }

    @tailrec
    def matchCsv(x: List[String]): Any = x match {
      case head::tail => runProducer(head);matchCsv(tail)
      case head::Nil => runProducer(head)
      case Nil => println("fin")
      case _ => println("error")
    }

    matchCsv(listFile)


  }
}

