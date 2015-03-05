package mypipe.api.producer

import com.typesafe.config.Config
import mypipe.api.event.Mutation

abstract class Producer(config: Config) {
  def queue(mutation: Mutation): Boolean
  def queueList(mutation: List[Mutation]): Boolean
  def flush(): Boolean
}

