package mypipe.producer

import mypipe.api._
import mypipe.kafka.{ KafkaProducer ⇒ KProducer, KafkaAvroVersionedSpecificProducer, KafkaAvroGenericProducer }
import com.typesafe.config.Config

abstract class KafkaProducer(mappings: List[Mapping], config: Config) extends Producer(mappings = null, config = config) {

  val metadataBrokers = config.getString("metadata-brokers")
  val producer: KProducer

  override def flush(): Boolean = {
    producer.flush
    true
  }

  override def queueList(mutations: List[Mutation[_]]): Boolean = {
    mutations.foreach(producer.send(_))
    true
  }

  override def queue(mutation: Mutation[_]): Boolean = {
    producer.send(mutation)
    true
  }

  override def toString(): String = {
    ""
  }

}

class GenericKafkaProducer(mappings: List[Mapping], config: Config) extends KafkaProducer(mappings, config) {
  override val producer = new KafkaAvroGenericProducer(metadataBrokers)
}

class SpecificVersionedKafkaProducer(mappings: List[Mapping], config: Config) extends KafkaProducer(mappings, config) {
  override val producer = new KafkaAvroVersionedSpecificProducer(metadataBrokers)
}