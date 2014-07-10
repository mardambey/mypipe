package mypipe.api

import com.typesafe.config.Config

abstract class Producer(config: Config) {
  def queue(mutation: Mutation[_]): Boolean
  def queueList(mutation: List[Mutation[_]]): Boolean
  def flush(): Boolean
}

