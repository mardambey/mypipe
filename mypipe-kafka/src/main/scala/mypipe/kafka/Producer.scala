package mypipe.kafka

import kafka.producer.{ Producer ⇒ KProducer, ProducerConfig }

import mypipe.api._
import java.util.Properties
import kafka.producer.KeyedMessage
import java.util.concurrent.LinkedBlockingQueue
import java.util
import java.util.logging.Logger
import org.apache.avro.specific.SpecificRecord
import org.apache.avro.Schema
import mypipe.avro.schema.SchemaRepository
import mypipe.avro.AvroVersionedRecordSerializer

/** A kafka producer that accepts beans extending Avro's SpecificRecord
 *  then uses the specified Avro schema repository to encode these beans before shipping
 *  them off into Kafka.
 *
 *  @param metadataBrokers list of Kafka brokers (host:port, comma separated)
 *  @param schemaRepoClient the Avro schema repository used to resolve schemas for encoding
 *  @tparam InputRecord the specific input record type this producer will handle
 */
class KafkaAvroVersionedProducer[InputRecord <: SpecificRecord](
    metadataBrokers: String,
    schemaRepoClient: SchemaRepository[Short, Schema]) {

  type KeyType = Array[Byte]
  type OutputMessageType = Array[Byte]

  val serializer: Serializer[InputRecord, Array[Byte]] = new AvroVersionedRecordSerializer[InputRecord](schemaRepoClient)

  val log = Logger.getLogger(getClass.getName)
  val properties = new Properties()
  properties.put("request.required.acks", "1")
  properties.put("metadata.broker.list", metadataBrokers)

  val conf = new ProducerConfig(properties)
  val producer = new KProducer[Array[Byte], Array[Byte]](conf)
  val queue = new LinkedBlockingQueue[KeyedMessage[KeyType, OutputMessageType]]()

  def send(topic: String, message: InputRecord) {
    val m = serializer.serialize(topic, message)
    if (m.isDefined) queue(topic, m.get)
  }

  def queue(topic: String, bytes: OutputMessageType) {
    queue.add(new KeyedMessage[KeyType, OutputMessageType](topic, bytes))
  }

  def flush: Boolean = {
    val s = new util.HashSet[KeyedMessage[KeyType, OutputMessageType]]
    queue.drainTo(s)
    val a = s.toArray[KeyedMessage[Array[Byte], Array[Byte]]](Array[KeyedMessage[Array[Byte], Array[Byte]]]())
    producer.send(a: _*)
    // TODO: return value properly
    true
  }
}

