package mypipe.kafka

import kafka.producer.{ Producer ⇒ KProducer, ProducerConfig }

import mypipe.api._
import java.util.Properties
import mypipe.kafka.types._
import kafka.producer.KeyedMessage
import java.util.concurrent.LinkedBlockingQueue
import java.util
import mypipe.avro.{ AvroVersionedGenericRecordMutationSerializer, AvroGenericRecordMutationSerializer }

abstract class Producer[OUTPUT] extends MutationSerializer[OUTPUT] {

  protected def serialize(input: Mutation[_]): OUTPUT

  def send(message: Mutation[_]) {
    val m = serialize(message)
    queue(getTopic(message), m)
  }

  def send(messages: Array[Mutation[_]]) {
    messages.foreach(m ⇒ queue(getTopic(m), serialize(m)))
  }

  def flush: Boolean
  protected def queue(topic: String, message: OUTPUT)
  protected def getTopic(message: Mutation[_]): String
}

package object types {
  type BinaryKeyedMessage = KeyedMessage[Array[Byte], Array[Byte]]
}

abstract class KafkaProducer(metadataBrokers: String)
    extends Producer[Array[Byte]] {

  val properties = new Properties()
  properties.put("request.required.acks", "1")
  properties.put("metadata.broker.list", metadataBrokers)

  val conf = new ProducerConfig(properties)
  val producer = new KProducer[Array[Byte], Array[Byte]](conf)
  val queue = new LinkedBlockingQueue[BinaryKeyedMessage]()

  override def queue(topic: String, bytes: Array[Byte]) {
    queue.add(new BinaryKeyedMessage(topic, null, bytes))
  }

  override def getTopic(message: Mutation[_]): String = message.table.db + "__" + message.table.name

  override def flush: Boolean = {
    val s = new util.HashSet[BinaryKeyedMessage]
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
class KafkaAvroGenericProducer(metadataBrokers: String)
    extends KafkaProducer(metadataBrokers)
    with AvroGenericRecordMutationSerializer {
}

/** Producers messages into Kafka with binary Avro
 *  encoding via specific records that have to be
 *  registered in an Avro Schema repository.
 *
 *  @param metadataBrokers where to find metadata about the Kafka cluster
 */
class KafkaAvroVersionedSpecificProducer(metadataBrokers: String)
  extends KafkaProducer(metadataBrokers)
  with AvroVersionedGenericRecordMutationSerializer
