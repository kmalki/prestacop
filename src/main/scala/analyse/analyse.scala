package analyse

import email.Consumer.alert.AutomaticEmailSender.sendMail
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{explode, split}
import org.apache.spark.sql.functions._

object analyse {

  def main(args: Array[String]): Unit = {

  val spark = SparkSession
    .builder()
    .appName("Analyse")
    .master("local[*]")
    .getOrCreate()



    val df = spark.read.option("header", "true").option("inferSchema","true")
      .csv("/home/yanis/Projet_Spark_Streaming/prestacop/archived_nypd_msg/part*.csv")


    //spark.sparkContext.setLogLevel("OFF")

    //df.printSchema()
    val df_input=df.filter(col("code").isNotNull).cache()

    println("nombre de lignes des csvs : " +df.count())


    /* L'infraction la plus courante */

    val df_infrac_courant = df_input.groupBy("code").count().sort(desc("count"))
    df_infrac_courant.show(5)


    /* Les violations en fonction des drones */

    val df_drone_violation = df_input.groupBy("droneId", "code").count().sort(desc("count"))
    df_drone_violation.show(10)


    /* Localisation des points d'infractions */

    val df_place_violation = df_input.groupBy("latitude", "longitude").count().sort(desc("count"))
    df_place_violation.show(10)

    /* Les drones qui envoient le plus d'alertes */

    val df_drone_alerte_most = df_input.filter(col("code")===842).groupBy("droneId","code").count().sort(desc("count"))
    df_drone_alerte_most.show(5)

    /* Le nombre d'infraction par année */
    val df_infraction_année = df.groupBy(col("code"),col("date")).count
    df_infraction_année.show(5)


    /* Top des 5 infractions les plus pratiqués */
    val dftop =  df.groupBy(col("code")).count.sort(desc("count")).limit(5)
    dftop.show()


    /* filtrer les infraction par une periode de date */
    /* les infractions qui ont eu lieux entre 2015 et 2016 */
    val df_date = df.filter(to_date(df("date"),"MM/dd/yyyy").between("2015-01-01","2016-01-01"))
    df_date.show(5)


    /* Infraction qui ont eu lieu à partir de janvier 2017 */
    val df_dategr = df.filter(to_date(df("date"),"MM/dd/yyyy").geq(lit("2017-01-01")))
    df_dategr.show(5)
    /* la batterie moyenne */
    val df_moyen_batterie = df.select(avg("battery"))

    sendMail("test", "Alerte drone "+"id_drone"+" Besoin d'intervention humaine" +"temps a mettre en variable scala.datetime")

  }


}
