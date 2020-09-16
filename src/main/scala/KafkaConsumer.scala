import java.util
import java.util.Properties

import com.typesafe.config.ConfigFactory
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}

import scala.collection.JavaConverters._

object KafkaConsumer {
  def main(args: Array[String]): Unit ={
    consumeMessage("first-topic")
  }

  def consumeMessage(topic: String): Unit ={
    val server = ConfigFactory.load().getString("kafka.bootstrap.server")
    val props = new Properties()
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, server)
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "1")

    val consumer = new KafkaConsumer[String,String](props)
    consumer.subscribe(util.Arrays.asList(topic))
    while(true){
      val records = consumer.poll(500).asScala
      for(record <- records.iterator){
        println(s"Received message: (${record.key()}, ${record.value()}) at offset ${record.offset()}")
      }
    }
  }
}
