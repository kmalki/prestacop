package stream

import java.beans.Encoder

import common.{Localisation, Message, ViolationMessage}
import email.AutomaticEmailSender
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{BooleanType, FloatType, IntegerType, StringType, StructType}

import scala.concurrent.duration._


object AlertHandlerConsumerStream {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("AlertHandler")
      .master("local[*]")
      .getOrCreate()

    val schema = (new StructType()
      .add("droneId", StringType)
      .add("position", (new StructType())
        .add("longitude", FloatType)
        .add("latitude", FloatType)
      )
      .add("date", StringType)
      .add("time", StringType)
      .add("violation", BooleanType)
      .add("violationMessage", (new StructType())
        .add("code", IntegerType)
        .add("imageId", StringType)
      )
      .add("battery", IntegerType)
      )

    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "drone-messages")
      .option("group.id", "alert")
      .option("auto.offset.reset", "latest")
      .option("enable.auto.commit", false: java.lang.Boolean)
      .load()

    df.selectExpr("CAST(value AS STRING)")
      .select(from_json(col("value"), schema).as("data")).select("data.*")
      .select(col("droneId"),
        col("position.*"),
        col("date"),
        col("time"),
        col("violation"),
        col("violationMessage.*"),
        col("battery")
      ).filter(col("violationMessage.code")===100)
      .writeStream
      .foreachBatch((df: DataFrame, id: Long) => {
        df.foreach(m => AutomaticEmailSender.sendMail(Message(
          m.getAs("violation"),
          m.getAs("droneId"),
          Some(ViolationMessage(
            m.getAs("code"),
            m.getAs("imageId")
          )),
          Localisation(
            m.getAs("longitude"),
            m.getAs("latitude")
          ),
          m.getAs("date"),
          m.getAs("time"),
          m.getAs("battery")
        ))
        )
      })
      .trigger(Trigger.ProcessingTime(1.second))
      .start()
      .awaitTermination()


  }
}