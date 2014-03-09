package mypipe.producer

import mypipe.api._
import java.util
import collection.JavaConverters._

class QueueProducer(queue: util.Queue[Mutation[_]]) extends Producer(mappings = null, config = null) {

  override def flush() {
  }

  override def queueList(mutationz: List[Mutation[_]]) {
    queue.addAll(mutationz.asJava)
  }

  override def queue(mutation: Mutation[_]) {
    queue.add(mutation)
  }

  override def toString(): String = {
    s"QueueProducer(elems=${queue.size})"
  }

}
