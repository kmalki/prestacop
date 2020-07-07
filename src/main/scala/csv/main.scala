object Main {
  def main(args: Array[String]): Unit = {

      val input = io.Source.fromFile("/home/hedi/IdeaProjects/drone/csv/project/Parking_Violations_Issued_-_Fiscal_Year_2015.csv").getLines()
      input.next()
      val producer = new Producertwo
      producer.produce(input)

  }
}
