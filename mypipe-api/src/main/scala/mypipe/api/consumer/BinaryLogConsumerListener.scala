package mypipe.api.consumer

import mypipe.api.data.Table
import mypipe.api.event.{ AlterEvent, Mutation }

trait BinaryLogConsumerListener[BinaryLogEvent] {
  def onStart(consumer: BinaryLogConsumer[BinaryLogEvent]) {}
  def onStop(consumer: BinaryLogConsumer[BinaryLogEvent]) {}
  def onMutation(consumer: BinaryLogConsumer[BinaryLogEvent], mutation: Mutation): Boolean = true
  def onMutation(consumer: BinaryLogConsumer[BinaryLogEvent], mutations: Seq[Mutation]): Boolean = true
  def onTableMap(consumer: BinaryLogConsumer[BinaryLogEvent], table: Table): Boolean = true
  def onTableAlter(consumer: BinaryLogConsumer[BinaryLogEvent], table: AlterEvent): Boolean = true
}

