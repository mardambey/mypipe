package mypipe.mysql

import mypipe.api.Mutation

trait BinlogConsumerListener {
  def onConnect(consumer: BinlogConsumer)
  def onDisconnect(consumer: BinlogConsumer)
  def onMutation(consumer: BinlogConsumer, mutation: Mutation[_]): Boolean
  def onMutation(consumer: BinlogConsumer, mutations: Seq[Mutation[_]]): Boolean
}

