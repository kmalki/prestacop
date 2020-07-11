package drone

object DroneRunner {
  def main(args: Array[String]): Unit = {
    val drone: Drone = new Drone
    drone.sendMessage()
  }
}
