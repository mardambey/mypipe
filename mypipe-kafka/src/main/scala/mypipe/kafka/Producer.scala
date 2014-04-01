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

abstract class Producer[InputMessageType, OutputMessageType] {

  val log = Logger.getLogger(getClass.getName)
  val serializer: Serializer[InputMessageType, OutputMessageType]

  def send(topic: String, message: InputMessageType) {
    val m = serializer.serialize(topic, message)
    if (m.isDefined) queue(topic, m.get)
  }

  def flush: Boolean
  protected def queue(topic: String, message: OutputMessageType)
}

abstract class KafkaProducer[InputMessageType, OutputMessageType](metadataBrokers: String) extends Producer[InputMessageType, OutputMessageType] {

  type KeyType = Array[Byte]

  val properties = new Properties()
  properties.put("request.required.acks", "1")
  properties.put("metadata.broker.list", metadataBrokers)

  val conf = new ProducerConfig(properties)
  val producer = new KProducer[Array[Byte], Array[Byte]](conf)
  val queue = new LinkedBlockingQueue[KeyedMessage[KeyType, OutputMessageType]]()

  override def queue(topic: String, bytes: OutputMessageType) {
    queue.add(new KeyedMessage[KeyType, OutputMessageType](topic, bytes))
  }

  override def flush: Boolean = {
    val s = new util.HashSet[KeyedMessage[KeyType, OutputMessageType]]
    queue.drainTo(s)
    val a = s.toArray[KeyedMessage[Array[Byte], Array[Byte]]](Array[KeyedMessage[Array[Byte], Array[Byte]]]())
    producer.send(a: _*)
    // TODO: return value properly
    true
  }

}

/** Produces messages into Kafka with binary Avro
 *  encoding via generic records (all tables use the
 *  same Avro bean).
 *
 *  @param metadataBrokers where to find metadata about the Kafka cluster
 */
class KafkaAvroVersionedProducer[InputRecord <: SpecificRecord](metadataBrokers: String,
                                                                schemaRepoClient: SchemaRepository[Short, Schema])
    extends KafkaProducer[InputRecord, Array[Byte]](metadataBrokers) {

  override val serializer: Serializer[InputRecord, Array[Byte]] = new AvroVersionedRecordSerializer[InputRecord](schemaRepoClient)
}

