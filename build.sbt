name := "prestacop"

version := "0.1"
scalaVersion := "2.12.8"

libraryDependencies ++= Seq(
  "ch.qos.logback" % "logback-classic" % "1.1.3" % Runtime,
  "au.com.bytecode" % "opencsv" % "2.4",
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % "2.4.0",
  "org.apache.spark" %% "spark-streaming" % "2.4.0",
  "org.apache.spark" %% "spark-core" % "2.4.0",
  "org.apache.kafka" %% "kafka" % "2.5.0",
  "com.typesafe.play" %% "play-json" % "2.8.0"
)