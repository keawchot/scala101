import org.apache.avro.Schema.Parser
import org.apache.avro.generic.GenericData
import io.confluent.kafka.serializers.{AbstractKafkaAvroSerDeConfig, KafkaAvroSerializer}
import org.apache.kafka.clients.producer._
import java.util
import java.util.HashMap

import com.landoop.evolution
import java.util.Properties

import org.apache.avro.specific.SpecificData
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.errors.SerializationException

object Main extends App {
  // ####################### Produce
//  var avroRecord = new GenericData.Record(evolution.getClassSchema)
//  avroRecord.put("name", "Hello World!")
//  avroRecord.put("number1", 1)
//  avroRecord.put("number2", 2.0f)
//

//  val MaxBytes = 1000000L
//  val props = new Properties()
//    props.put("bootstrap.servers", "internal-kafka-broker-1171600617.ap-southeast-1.elb.amazonaws.com:9092")
//    props.put("acks", "all")
//    props.put("retries", "2")
//    props.put("buffer.memory", "4500000")
//    props.put("linger.ms", "500")
//    props.put("key.serializer",
//      "org.apache.kafka.common.serialization.StringSerializer")
//    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
//      classOf[KafkaAvroSerializer])
//    props.put("schema.registry.url", "https://dev-kafka-schema-registry.kpc.report")
//
//  val kafkaProducer = new KafkaProducer[Object, Object](props)
//  val record: ProducerRecord[Object, Object] = new ProducerRecord("keaw-forward", avroRecord.get("name"), avroRecord)
//  kafkaProducer.send(record)
//  kafkaProducer.flush()
//  kafkaProducer.close()

  //######################## Consumer

  val props = new Properties()

  import org.apache.kafka.clients.consumer.ConsumerConfig

  props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, args(0))//"internal-kafka-broker-1171600617.ap-southeast-1.elb.amazonaws.com:9092")
  props.put(ConsumerConfig.GROUP_ID_CONFIG, args(3))


  props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
  props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaAvroDeserializer")
  props.put("schema.registry.url", args(1))//"https://dev-kafka-schema-registry.kpc.report")

  props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
  val topic = args(2)
  val consumer = new KafkaConsumer[String, String](props)
  consumer.subscribe(util.Arrays.asList(topic))
  while(true) {
    val records = consumer.poll(2000)

    //print each received record
    records.forEach(record => {
      println(record.value())
    })

    //commit offsets on last poll
    consumer.commitSync()
  }
}