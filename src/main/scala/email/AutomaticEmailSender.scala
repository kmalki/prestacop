package email

import java.util.Properties

import javax.mail.{Message, Session, Transport}
import javax.mail.internet.{InternetAddress, MimeMessage}



object AutomaticEmailSender {
  val host = "smtp.gmail.com"
  val port = "587"

  val address = "yakaheesgi@gmail.com"
  val username = "yakaheesgi"
  val password = "YaKaHe!?1996"

  val addressTo = "poc.prestacop@gmail.com"

  def sendMail(msgDrone: common.Message): Unit = {
    val properties: Properties = new Properties
    properties.put("mail.smtp.port", port)
    properties.put("mail.smtp.auth", "true")
    properties.put("mail.smtp.starttls.enable", "true")

    val session: Session = Session.getDefaultInstance(properties, null)
    val message: MimeMessage = new MimeMessage(session)

    message.addRecipient(Message.RecipientType.TO, new InternetAddress(addressTo))
    message.setSubject(
      s"""Alert to handle : ${{msgDrone.date}} //
         |${{msgDrone.time}} //
         |${msgDrone.droneId} //
         |(${{msgDrone.position.longitude}},${{msgDrone.position.latitude}}) //
         |""".stripMargin)
    message.setContent(s"""Image Id of the ${msgDrone.violationMessage.get.imageId}""", "text/html")

    val transport: Transport = session.getTransport("smtp")
    transport.connect(host, username, password)
    transport.sendMessage(message, message.getAllRecipients)
  }

}