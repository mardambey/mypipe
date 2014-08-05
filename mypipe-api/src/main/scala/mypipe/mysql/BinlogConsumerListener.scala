package mypipe.mysql

import mypipe.api.{ Table, Mutation }

trait BinlogConsumerListener {
  def onConnect(consumer: BinlogConsumer) {}
  def onDisconnect(consumer: BinlogConsumer) {}
  def onMutation(consumer: BinlogConsumer, mutation: Mutation[_]): Boolean = true
  def onMutation(consumer: BinlogConsumer, mutations: Seq[Mutation[_]]): Boolean = true
  def onTableMap(consumer: BinlogConsumer, table: Table) {}
}

