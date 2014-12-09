package mypipe.producer

import mypipe.api._
import java.util
import mypipe.api.event.Mutation
import mypipe.api.producer.Producer

import collection.JavaConverters._

class QueueProducer(queue: util.Queue[Mutation[_]]) extends Producer(config = null) {

  override def flush() = true

  override def queueList(mutationz: List[Mutation[_]]): Boolean = {
    queue.addAll(mutationz.asJava)
    true
  }

  override def queue(mutation: Mutation[_]): Boolean = {
    queue.add(mutation)
    true
  }

  override def toString(): String = {
    s"QueueProducer(elems=${queue.size})"
  }

}
