package streamstorage

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._
import scala.concurrent.duration._

object CsvConsumerStreamStorage {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("CsvStreamProcessing")
      .master("local[*]")
      .getOrCreate()

    val schema = (new StructType()
      .add("droneId",StringType)
      .add("position",(new StructType())
        .add("longitude",FloatType)
        .add("latitude",FloatType)
      )
      .add("date",StringType)
      .add("time",StringType)
      .add("violation",BooleanType)
      .add("violationMessage",(new StructType())
        .add("code",IntegerType)
        .add("imageId",StringType)
      )
      .add("battery",IntegerType)
      )

    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "csv-messages")
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
      )
      .writeStream
      .option("format", "append")
      .format("csv")
      .option("header", value = true)
      .option("checkpointLocation", "tmp/csv_stream/sparkcheckpoints")
      .option("path", "archived_nypd_msg/")
      .outputMode("append")
      .trigger(Trigger.ProcessingTime(1.minutes))
      .start()
      .awaitTermination()

  }
}
