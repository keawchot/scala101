import org.apache.avro.generic.GenericRecord
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.serialization.Serializer
import java.util.Collections
import java.util
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient


class GenericAvroSerde(genericAvroSerializer: GenericAvroSerializer, genericAvroDeserializer: GenericAvroDeserializer) extends Serde[GenericRecord] {
  private final var inner: Serde[GenericRecord] = Serdes.serdeFrom(genericAvroSerializer, genericAvroDeserializer)

  def this() {
    this(new GenericAvroSerializer, new GenericAvroDeserializer)
  }

  def this(client: SchemaRegistryClient) {
    this(new GenericAvroSerializer(client), new GenericAvroDeserializer(client, Collections.emptyMap()))
  }

  def this(client: SchemaRegistryClient, props: util.Map[String, Unit]) {
    this(new GenericAvroSerializer(client), new GenericAvroDeserializer(client, props))
  }

  override def serializer: Serializer[GenericRecord] = inner.serializer

  override def deserializer: Deserializer[GenericRecord] = inner.deserializer

  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {
    inner.serializer.configure(configs, isKey)
    inner.deserializer.configure(configs, isKey)
  }

  override def close(): Unit = {
    inner.serializer.close()
    inner.deserializer.close()
  }
}