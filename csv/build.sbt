name := "Drone_msg"

version := "0.1"

scalaVersion := "2.13.2"

val sparkVersion = "2.4.5"

libraryDependencies ++= Seq(
  "org.apache.kafka" % "kafka_2.11" % "1.1.1",
  "ch.qos.logback" % "logback-classic" % "1.1.3" % Runtime,
  "au.com.bytecode" % "opencsv" % "2.4"
)