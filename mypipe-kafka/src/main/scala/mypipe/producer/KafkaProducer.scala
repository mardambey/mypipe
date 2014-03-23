package mypipe.producer

import mypipe.api._
import mypipe.kafka.{ KafkaProducer ⇒ KProducer }
import com.typesafe.config.Config
import java.util.concurrent.LinkedBlockingQueue
import java.util

class KafkaProducer(config: Config) extends Producer(mappings = null, config = config) {

  val metadataBrokers = config.getString("metadata-brokers")
  // TODO: don't use a queue here, use an actor instead
  val queue = new LinkedBlockingQueue[Mutation[_]]()
  val producer = new KProducer(metadataBrokers)

  override def flush(): Boolean = {
    val s = new util.HashSet[Mutation[_]]
    queue.drainTo(s)
    val a = s.toArray[Mutation[_]](Array[Mutation[_]]())
    producer.send(a)
    true
  }

  override def queueList(mutations: List[Mutation[_]]): Boolean = {
    mutations.foreach(queue.add(_))
    true
  }

  override def queue(mutation: Mutation[_]): Boolean = {
    queue.add(mutation)
    true
  }

  override def toString(): String = {
    ""
  }

}
