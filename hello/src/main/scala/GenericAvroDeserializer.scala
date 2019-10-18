import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient
import io.confluent.kafka.serializers.KafkaAvroDeserializer
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.common.serialization.Deserializer
import java.util


class GenericAvroDeserializer() extends Deserializer[GenericRecord] {
  var inner: KafkaAvroDeserializer = new KafkaAvroDeserializer

  def this(client: SchemaRegistryClient) {
    this()
    inner = new KafkaAvroDeserializer(client)
  }

  def this(client: SchemaRegistryClient, props: util.Map[String, _]) {
    this()
    inner = new KafkaAvroDeserializer(client, props)
  }

  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {
    inner.configure(configs, isKey)
  }

  override def deserialize(s: String, bytes: Array[Byte]): GenericRecord = inner.deserialize(s, bytes).asInstanceOf[GenericRecord]

  override def close(): Unit = {
    inner.close()
  }
}