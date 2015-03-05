package mypipe.api.consumer

import mypipe.api.data.Table
import mypipe.api.event.Mutation

trait BinaryLogConsumerListener {
  def onConnect(consumer: BinaryLogConsumer) {}
  def onDisconnect(consumer: BinaryLogConsumer) {}
  def onMutation(consumer: BinaryLogConsumer, mutation: Mutation): Boolean = true
  def onMutation(consumer: BinaryLogConsumer, mutations: Seq[Mutation]): Boolean = true
  def onTableMap(consumer: BinaryLogConsumer, table: Table) {}
  def onTableAlter(consumer: BinaryLogConsumer, table: Table) {}
}

