package mypipe.api.consumer

import mypipe.api.data.Table
import mypipe.api.event.Mutation

trait BinaryLogConsumerListener {
  def onConnect(consumer: AbstractBinaryLogConsumer) {}
  def onDisconnect(consumer: AbstractBinaryLogConsumer) {}
  def onMutation(consumer: AbstractBinaryLogConsumer, mutation: Mutation[_]): Boolean = true
  def onMutation(consumer: AbstractBinaryLogConsumer, mutations: Seq[Mutation[_]]): Boolean = true
  def onTableMap(consumer: AbstractBinaryLogConsumer, table: Table) {}
  def onTableAlter(consumer: AbstractBinaryLogConsumer, table: Table) {}
}

