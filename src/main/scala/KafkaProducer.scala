import java.util.Properties
import java.io.File
import com.typesafe.config.ConfigFactory
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import scala.io.Source

object KafkaProducer {
  def main(args: Array[String]): Unit ={
    val dir = "/Users/vishu/current/files"
    val topic = "first-topic"
    produceMessages(topic,dir)
  }

  def produceMessages(topic : String, dir : String): Unit ={
    val server = ConfigFactory.load().getString("kafka.bootstrap.server")
    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, server)
    props.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaProducer")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    val producer = new KafkaProducer[String,String](props)
    val filesList = getListOfFiles(dir)
    println(filesList)
    for(file <- filesList){
      val fileName = getFileName(file)
      if (fileName != ".DS_Store"){
        val fileContent = getFileContent(file)
        val data = new ProducerRecord[String,String](topic, fileName, fileContent)
        producer.send(data)
      }
    }
    producer.close()
  }

  def getListOfFiles(dir: String): List[String] = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(_.isFile).map(_.toString).toList
    } else {
      List[String]()
    }
  }

  def getFileName(inputFile : String): String ={
    val fileName = inputFile.split('/').last
    fileName
  }

  def getFileContent(inputFile : String): String ={
    val source = Source.fromFile(inputFile)
    val fileContent = source.getLines().mkString
    fileContent
  }
}
