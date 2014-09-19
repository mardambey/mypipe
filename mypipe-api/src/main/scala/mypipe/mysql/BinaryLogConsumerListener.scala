package mypipe.mysql

import mypipe.api.{ Table, Mutation }

trait BinaryLogConsumerListener {
  def onConnect(consumer: BaseBinaryLogConsumer) {}
  def onDisconnect(consumer: BaseBinaryLogConsumer) {}
  def onMutation(consumer: BaseBinaryLogConsumer, mutation: Mutation[_]): Boolean = true
  def onMutation(consumer: BaseBinaryLogConsumer, mutations: Seq[Mutation[_]]): Boolean = true
  def onTableMap(consumer: BaseBinaryLogConsumer, table: Table) {}
}

