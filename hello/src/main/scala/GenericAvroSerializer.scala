import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient
import io.confluent.kafka.serializers.KafkaAvroSerializer
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.common.serialization.Serializer
import java.util

class GenericAvroSerializer() extends Serializer[GenericRecord] {
  private var inner: KafkaAvroSerializer = new KafkaAvroSerializer

  def this(client: SchemaRegistryClient) {
    this()
    inner = new KafkaAvroSerializer(client)
  }

  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {
    inner.configure(configs, isKey)
  }

  override def serialize(topic: String, record: GenericRecord): Array[Byte] = inner.serialize(topic, record)

  override def close(): Unit = {
    inner.close()
  }
}