package mypipe.mysql

import mypipe.api.consumer.{ BinaryLogConsumer, BinaryLogConsumerListener, AbstractBinaryLogConsumer }
import mypipe.api.data.Table
import mypipe.api.event.{ Mutation, AlterEvent, TableMapEvent }

trait ConfigBasedErrorHandlingBehaviour extends BinaryLogConsumer {

  protected def handleError(listener: BinaryLogConsumerListener, mutation: Mutation) {
    // TODO: implement config based error handling
    //  val handlers = Conf.loadClassesForKey[BinaryLogConsumerErrorHandler]("mypipe.")
  }
}

trait CacheableTableMapBehaviour extends BinaryLogConsumer {

  protected var tableCache = new TableCache(hostname, port, username, password)

  protected def findTable(tableMapEvent: TableMapEvent): Table = {
    val table = tableCache.addTableByEvent(tableMapEvent)
    table
  }

  protected def getTableById(tableId: java.lang.Long): Table =
    tableCache.getTable(tableId).get

  protected def findTable(event: AlterEvent): Option[Table] = {
    // FIXME: this sucks and needs to be parsed properly
    val tableName = event.sql.split(" ")(2)
    tableCache.refreshTable(event.database, tableName)
  }
}