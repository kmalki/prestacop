package common

case class Localisation(var longitude: Float, var latitude: Float)
case class ViolationMessage(var code: Int, var imageId: String)
case class Message(var violation: Boolean, var droneId: String, var violationMessage: Option[ViolationMessage],
                   var position: Localisation, var date: String, var time: String, var battery: Int)
