# KafkaDemo

This application can be used to produce messages to a Kafka topic, consume messages from a Kafka topic and process those messages using Spark and display the results on the console.

## Requirements

The following tools are required:
- Java SDK      : 8
- Scala Version : 2.11.12
- sbt Version   : 0.13.18
- Apache Kafka  : 2.4.1

## Dependencies

Add the following dependencies:
- kafka-clients : 2.4.1
- spark-streaming : 2.4.1
- spark-kafka-streaming : 2.4.1
- config : 1.3.2

## Getting Started

1. Clone the repository to your system
2. Make sure you have installed required tools in the system
3. Unzip the downloaded `kafka_2.11-2.4.1` zip file and open the directory in Terminal
4. Start zookeeper server by executing the command `./bin/zookeeper-server-start.sh cong/zookeeper.properties` in Terminal
5. Start kafka server by executing the command `./bin/kafka-server-start.sh config/server.properties` in Terminal
6. Now launch `sbt` at the root of the project directory. (make sure you have stable internet while downloading dependencies)
7. Now type `compile` command to compile the src.
8. Now type `run` command to launch the application.
