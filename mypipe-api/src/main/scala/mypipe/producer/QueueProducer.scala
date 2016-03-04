package mypipe.producer

import java.util
import mypipe.api.event.{ AlterEvent, Mutation }
import mypipe.api.producer.Producer

import collection.JavaConverters._

class QueueProducer(queue: util.Queue[Mutation]) extends Producer(config = null) {

  override def flush() = true

  override def handleAlter(event: AlterEvent): Boolean = true

  override def queueList(mutationz: List[Mutation]): Boolean = {
    queue.addAll(mutationz.asJava)
    true
  }

  override def queue(mutation: Mutation): Boolean = {
    queue.add(mutation)
    true
  }

  override def toString: String = {
    s"QueueProducer(elems=${queue.size})"
  }

}
