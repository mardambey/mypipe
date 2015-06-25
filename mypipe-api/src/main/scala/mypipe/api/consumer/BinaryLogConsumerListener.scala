package mypipe.api.consumer

import mypipe.api.data.Table
import mypipe.api.event.{ AlterEvent, Mutation }

trait BinaryLogConsumerListener[BinaryLogEvent, BinaryLogPosition] {
  def onConnect(consumer: BinaryLogConsumer[BinaryLogEvent, BinaryLogPosition]) {}
  def onDisconnect(consumer: BinaryLogConsumer[BinaryLogEvent, BinaryLogPosition]) {}
  def onMutation(consumer: BinaryLogConsumer[BinaryLogEvent, BinaryLogPosition], mutation: Mutation): Boolean = true
  def onMutation(consumer: BinaryLogConsumer[BinaryLogEvent, BinaryLogPosition], mutations: Seq[Mutation]): Boolean = true
  def onTableMap(consumer: BinaryLogConsumer[BinaryLogEvent, BinaryLogPosition], table: Table): Boolean = true
  def onTableAlter(consumer: BinaryLogConsumer[BinaryLogEvent, BinaryLogPosition], table: AlterEvent): Boolean = true
}

