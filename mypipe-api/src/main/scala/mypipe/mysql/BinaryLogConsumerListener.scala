package mypipe.mysql

import mypipe.api.{ Table, Mutation }

trait BinaryLogConsumerListener {
  def onConnect(consumer: BinaryLogConsumerTrait) {}
  def onDisconnect(consumer: BinaryLogConsumerTrait) {}
  def onMutation(consumer: BinaryLogConsumerTrait, mutation: Mutation[_]): Boolean = true
  def onMutation(consumer: BinaryLogConsumerTrait, mutations: Seq[Mutation[_]]): Boolean = true
  def onTableMap(consumer: BinaryLogConsumerTrait, table: Table) {}
  def onTableAlter(consumer: BinaryLogConsumerTrait, table: Table) {}
}

