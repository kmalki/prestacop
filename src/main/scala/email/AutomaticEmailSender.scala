package email

import java.util.Properties;
import javax.activation._
import javax.mail._
import scala.collection.JavaConversions._
import javax.mail.internet._
import java.util.Date

object AutomaticEmailSender {

  class MailAgent(to: String,
                  cc: String,
                  bcc: String,
                  from: String,
                  subject: String,
                  content: String,
                  attached: Map[String, String],
                  smtpHost: String) {
    var message: Message = null

    message = createMessage
    message.setFrom(new InternetAddress(from))
    setToCcBccRecipients

    message.setSentDate(new Date())
    message.setSubject(subject)

    var multipart = new MimeMultipart()
    val bodyPart = new MimeBodyPart()
    bodyPart.setContent(content, "text/html")
    multipart.addBodyPart(bodyPart)
    attached.foreach {
      file => {
        val filePart = new MimeBodyPart()
        val source = new FileDataSource(file._2)
        filePart.setDataHandler(new DataHandler(source))
        filePart.setFileName(file._1)
        multipart.addBodyPart(filePart)
      }
    }
    message.setContent(multipart)

    def sendMessage {
      Transport.send(message)
    }

    def createMessage: Message = {
      val properties = new Properties()
      properties.put("mail.smtp.host", smtpHost)
      properties.put("mail.smtp.auth", "false")
      val session = Session.getDefaultInstance(properties, null)
      session.setDebug(true)
      return new MimeMessage(session)
    }

    def setToCcBccRecipients {
      setMessageRecipients(to, Message.RecipientType.TO)
      if (cc != null) {
        setMessageRecipients(cc, Message.RecipientType.CC)
      }
      if (bcc != null) {
        setMessageRecipients(bcc, Message.RecipientType.BCC)
      }
    }

    def setMessageRecipients(recipient: String, recipientType: Message.RecipientType) {
      val addressArray = buildInternetAddressArray(recipient).asInstanceOf[Array[Address]]
      if ((addressArray != null) && (addressArray.length > 0)) {
        message.setRecipients(recipientType, addressArray)
      }
    }

    def buildInternetAddressArray(address: String): Array[InternetAddress] = {
      return InternetAddress.parse(address)
    }

  }

  def createAndSendMessage(to: String,
                           cc: String,
                           bcc: String,
                           from: String,
                           subject: String,
                           content: String,
                           attached: Map[String, String],
                           smtpHost: String
                          ): Unit = {
    val email = new MailAgent(to, cc, bcc, from, subject, content, attached, smtpHost)

    email.sendMessage
  }

}