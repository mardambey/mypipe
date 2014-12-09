package mypipe.mysql

import mypipe.api.consumer.{ BinaryLogConsumerListener, AbstractBinaryLogConsumer }
import mypipe.api.data.Table
import mypipe.api.event.{ Mutation, AlterEvent, TableMapEvent }

trait NoErrorHandlingBehaviour extends AbstractBinaryLogConsumer {
  override protected def handleError(listener: BinaryLogConsumerListener, mutation: Mutation[_]) {}
}

trait ConfigBasedErrorHandlingBehaviour extends AbstractBinaryLogConsumer {
  override protected def handleError(listener: BinaryLogConsumerListener, mutation: Mutation[_]) {
    // TODO: implement config based error handling
    //  val handlers = Conf.loadClassesForKey[BinaryLogConsumerErrorHandler]("mypipe.")
  }
}

trait CacheableTableMapBehaviour extends AbstractBinaryLogConsumer {
  protected var tableCache = new TableCache(hostname, port, username, password)

  override protected def findTable(tableMapEvent: TableMapEvent): Table = {
    val table = tableCache.addTableByEvent(tableMapEvent)
    table
  }

  override protected def getTableById(tableId: java.lang.Long): Table =
    tableCache.getTable(tableId).get

  override protected def findTable(event: AlterEvent): Option[Table] = {
    // FIXME: this sucks and needs to be parsed properly
    val tableName = event.sql.split(" ")(2)
    tableCache.refreshTable(event.database, tableName)
  }
}