object Main {
  def main(args: Array[String]): Unit = {
    val drone: Drone = new Drone
    while(true){
      drone.sendMessage()
      Thread.sleep(5000)
    }
  }
}
