import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

object KafkaSparkConsumer {
  def main(args : Array[String]): Unit ={
    val brokerId = "localhost:9092"
    val groupId = "sparkgroup"
    val topics = "first-topic"
    val topicSet = topics.split(",").toSet
    val kafkaParams = Map[String,Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokerId,
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest",
      ConsumerConfig.GROUP_ID_CONFIG -> groupId,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer]
    )
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Kafka Spark App")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    val sc = ssc.sparkContext
    sc.setLogLevel("OFF")
    val message = KafkaUtils.createDirectStream[String,String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String,String](topicSet,kafkaParams)
    )

    //count the number of times a word appear span of 5 seconds
    val words = message.map(_.value()).flatMap(_.split(" "))
    val wordCount = words.map(x => (x,1)).reduceByKey(_ + _)
    //print wordCount on console
    wordCount.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
