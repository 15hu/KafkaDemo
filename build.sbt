name := "KafkaApp"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies += "com.typesafe" % "config" % "1.3.2"
libraryDependencies += "org.apache.kafka" % "kafka-clients" % "2.4.1"
libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka-0-10" % "2.4.1"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.4.1"