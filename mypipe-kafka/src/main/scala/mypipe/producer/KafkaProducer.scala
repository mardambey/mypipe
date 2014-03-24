package mypipe.producer

import mypipe.api._
import mypipe.kafka.{ KafkaAvroGenericProducer ⇒ KProducer }
import com.typesafe.config.Config
import java.util.concurrent.LinkedBlockingQueue
import java.util

class KafkaProducer(config: Config) extends Producer(mappings = null, config = config) {

  val metadataBrokers = config.getString("metadata-brokers")
  val producer = new KProducer(metadataBrokers)

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
