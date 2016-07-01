package mypipe.api.producer

import com.typesafe.config.Config
import mypipe.api.event.{AlterEvent, Mutation}

abstract class Producer(config: Config) {
  def queue(mutation: Mutation): Boolean
  def queueList(mutation: List[Mutation]): Boolean
  def flush(): Boolean
  def handleAlter(event: AlterEvent): Boolean
}

