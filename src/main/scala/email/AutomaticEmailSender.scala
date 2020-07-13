package email

package Consumer.alert


import java.util.Properties
import javax.mail.{Message, Session}
import javax.mail.internet.{InternetAddress, MimeMessage}




object AutomaticEmailSender {
  val host = "smtp.gmail.com"
  val port = "587"

  val address = "yakaheesgi@gmail.com"
  val username = "yakaheesgi"
  val password = "YaKaHe!?1996"

  def sendMail(text:String, subject:String) = {
    val properties = new Properties()
    properties.put("mail.smtp.port", port)
    properties.put("mail.smtp.auth", "true")
    properties.put("mail.smtp.starttls.enable", "true")

    val session = Session.getDefaultInstance(properties, null)
    val message = new MimeMessage(session)
    message.addRecipient(Message.RecipientType.TO, new InternetAddress(address));
    message.setSubject(subject)
    message.setContent(text, "text/html")

    val transport = session.getTransport("smtp")
    transport.connect(host, username, password)
    transport.sendMessage(message, message.getAllRecipients)
  }

  sendMail("test", "Alerte drone "+"id_drone"+" Besoin d'intervention humaine" +"temps a mettre en variable scala.datetime")

}