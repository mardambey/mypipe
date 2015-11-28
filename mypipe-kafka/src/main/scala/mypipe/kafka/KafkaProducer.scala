package mypipe.kafka

import kafka.producer.{ Producer ⇒ KProducer, ProducerConfig }

import java.util.Properties
import kafka.producer.KeyedMessage
import java.util.concurrent.LinkedBlockingQueue
import java.util
import java.util.logging.Logger

class KafkaProducer[MessageType](metadataBrokers: String) {

  type KeyType = Array[Byte]

  val log = Logger.getLogger(getClass.getName)
  val properties = new Properties()
  properties.put("request.required.acks", "1")
  properties.put("metadata.broker.list", metadataBrokers)

  val conf = new ProducerConfig(properties)
  val producer = new KProducer[Array[Byte], Array[Byte]](conf)
  val queue = new LinkedBlockingQueue[KeyedMessage[KeyType, MessageType]]()

  def queue(topic: String, bytes: MessageType) {
    queue.add(new KeyedMessage[KeyType, MessageType](topic, bytes))
  }

  def queue(topic: String, messageKey: Array[Byte], bytes: MessageType) {
    queue.add(new KeyedMessage[KeyType, MessageType](topic, messageKey, bytes))
  }

  def flush: Boolean = {
    val s = new util.HashSet[KeyedMessage[KeyType, MessageType]]
    queue.drainTo(s)
    val a = s.toArray[KeyedMessage[Array[Byte], Array[Byte]]](Array[KeyedMessage[Array[Byte], Array[Byte]]]())
    producer.send(a: _*)
    true
  }
}

