import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

object KafkaProducer {
  def main(args: Array[String]): Unit ={
    produceMessage("first-topic", "samplekey", "samplevalue")
  }

  def produceMessage(topic:String, key:String, value:String): Unit ={
    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaProducer")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    val producer = new KafkaProducer[String,String](props)

    val data = new ProducerRecord[String,String](topic, key, value)
    producer.send(data)
    producer.close()
  }
}
