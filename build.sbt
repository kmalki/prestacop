name := "prestacop"

version := "0.1"
scalaVersion := "2.12.8"

libraryDependencies ++= Seq(
  "ch.qos.logback" % "logback-classic" % "1.1.3" % Runtime,
  "au.com.bytecode" % "opencsv" % "2.4",
  "org.apache.spark" %% "spark-core" % "2.4.0",
  "org.apache.spark" %% "spark-sql" % "2.4.0",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % "2.4.0",
  "org.apache.kafka" %% "kafka" % "2.5.0",
  "com.typesafe.play" %% "play-json" % "2.8.0",
  "javax.mail"        % "mail"           % "1.4.1",
  "org.apache.commons" % "commons-email" % "1.5"

)