package mypipe.kafka

import kafka.producer.{Producer ⇒ KProducer, KeyedMessage, ProducerConfig}

import mypipe.api.{InsertMutation, UpdateMutation, DeleteMutation, Mutation}

import java.util.Properties
import mypipe.kafka.types._

trait MessageQueue[MESSAGE] {
  def produce(message: MESSAGE)
}

abstract class Producer[OUTPUT] {

  def serialize(input: Mutation[_]) : OUTPUT

  def send(message: Mutation[_]) {
    val m = serialize(message)
    produce(m)
  }

  def produce(message: OUTPUT)
}

trait MutationSerializer[OUTPUT] {
  def serialize(input: Mutation[_]) : OUTPUT
}

trait BinaryKeyedMessageMutationSerializer[PKEY, MESSAGE] extends MutationSerializer[BinaryKeyedMessage] {

  def serialize(mutation: Mutation[_]) : BinaryKeyedMessage = {

    mutation match {

      case i: InsertMutation => new KeyedMessage[Array[Byte], Array[Byte]](s"${i.table.db}:{$i.table.name}")
      case u: UpdateMutation =>
      case d: DeleteMutation =>
    }

  }
}

trait KafkaMessageQueue[PKEY, MESSAGE] extends MessageQueue[KeyedMessage[PKEY, MESSAGE]] {

  val metadataBrokers: String
  val properties = new Properties()
  properties.put("request.required.acks", "1")
  properties.put("metadata.broker.list", metadataBrokers)

  val conf = new ProducerConfig(properties)

  val producer = new KProducer[PKEY, MESSAGE](conf)

  def produce(message: KeyedMessage[PKEY, MESSAGE]) {
    producer.send(message)
  }

}

package object types {
  type BinaryKeyedMessage = KeyedMessage[Array[Byte], Array[Byte]]
}

class KafkaProducer(override val metadataBrokers: String)
  extends Producer[BinaryKeyedMessage]
  with KafkaMessageQueue[Array[Byte], Array[Byte]]
  with BinaryKeyedMessageMutationSerializer[Array[Byte], Array[Byte]]
