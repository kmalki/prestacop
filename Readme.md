# Présentation : Projet en binôme de fin de module de Spark de première année de mastère :

Le but de ce projet était d’implémenter Spark Streaming et Kafka dans un contexte d’un drone policier qui détectait et envoyait des infractions. 

Il fallait implémenter le drone et l’envoie des infractions via un producer Kafka, ainsi qu’un consumer Spark Streaming permettant la récupération de ces messages depuis Kafka et le stockage au format csv.

Il fallait également implémenter le producer via Kafka d’un lourd fichier csv d’historique d’infractions en y ajoutant des fausses données comme le numéro du drone, ou encore la localisation, et le consumer Spark Streaming qui convient pour stocker à nouveau en csv. Ce processus faisait référence à une problématique imposée qui simulait que la police devait transférer ce fichier d’historisation à la nouvelle entreprise qui mettait en place les drones afin d’améliorer leurs statistiques, sauf que les pc étaient trop faibles pour transférer un fichier aussi lourd. 

Enfin, développer un job d’analyse Spark sur les données du csv d’historique d’infractions récupérées par le consumer, comme par exemple le code d’infraction la plus courante, ou encore la répartition des codes d’infraction en fonction de chaque drone.

Stack technique :
- Scala
- Spark & Spark Streaming
- Kafka


## Prestacop

This project is a final semester project for our course of Spark.

We had to implement Spark Streaming and Kafka in the situation of a drone police detecting and sending infractions
city of New York.

We had to implement the drone which send random infractions in Scala.

We also had to implement a data pipe line which persists all those messages into our file system.

Finally, an other data pipe line to transfer huge Infration of NY CSVs through Kafka and then store them as for the drone messages.

### How to

Run Zookeeper service and Kafka with docker-compose

`docker-compose up -d`

#### Drone Pipeline

Run the drone producer named DroneRunner.scala and the kafka consumers DroneConsumerStreamStorage.scala to store the drone messages
in new csvs and AlertHandlerConsumerStream.scala to handle the alert messages and send email for every alert on a fictive email that we created (can be changed in `email/AutomaticEmailSender.scala`).

New CSV files will be stored in drone_msg.

### NY CSV Pipeline

Put the CSV found on this Kaggle : https://www.kaggle.com/new-york-city/nyc-parking-tickets in the `nypd_csv/` folder and run
both ProducerCsvRunner.scala (producer) and CsvConsumerStreamStorage.scala (consumer). 

New CSV files will be stored in `archived_nypd_msg`.

### Analysis using Spark

We also did analysis on the NY Csvs recreated after processing through Kafka. For this, run Analyse.scala.

### SBT

If using sbt, proceed in differents sbt shell as :

`sbt compile`

`sbt runMain drone.DroneRunner`

`sbt runMain stream.CsvConsumerStreamStorage`

`sbt runMain stream.DroneRunner`

`sbt runMain stream.DroneRunner`

`sbt runMain analyse.Analyse`
