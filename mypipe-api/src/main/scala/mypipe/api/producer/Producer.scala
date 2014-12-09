package mypipe.api.producer

import com.typesafe.config.Config
import mypipe.api.event.Mutation

abstract class Producer(config: Config) {
  def queue(mutation: Mutation[_]): Boolean
  def queueList(mutation: List[Mutation[_]]): Boolean
  def flush(): Boolean
}

