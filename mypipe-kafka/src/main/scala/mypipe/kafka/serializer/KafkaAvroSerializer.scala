package mypipe.kafka.serializer

import java.util

import org.apache.kafka.common.serialization.Serializer

class KafkaAvroSerializer extends Serializer[Array[Byte]] {
  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = ???

  override def serialize(topic: String, data: Array[Byte]): Array[Byte] = ???

  override def close(): Unit = ???
}
