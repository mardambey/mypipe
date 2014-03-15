package mypipe.api

import com.typesafe.config.Config

abstract class Producer(mappings: List[Mapping], config: Config) {
  def queue(mutation: Mutation[_]): Boolean
  def queueList(mutation: List[Mutation[_]]): Boolean
  def flush()
}

