package mypipe.mysql

import com.github.shyiko.mysql.binlog.event.Event

import mypipe.api.{ TableMapEvent, Mutation, Table }

trait BinaryLogRawConsumerTrait {
  protected def handleEvent(event: Event)
}

trait BinaryLogConsumerTrait {

  val hostname: String
  val port: Int
  val username: String
  val password: String

  protected def handleError(listener: BinaryLogConsumerListener, mutation: Mutation[_])
  protected def handleTableMap(event: TableMapEvent): Table
  protected def handleMutation(mutation: Mutation[_]): Boolean

  protected def handleDisconnect()
  protected def handleConnect()

  protected def getTableById(tableId: java.lang.Long): Table
}

trait NoErrorHandling extends BinaryLogConsumerTrait {
  override protected def handleError(listener: BinaryLogConsumerListener, mutation: Mutation[_]) {}
}

trait ConfigBasedErrorHandling extends BinaryLogConsumerTrait {
  override protected def handleError(listener: BinaryLogConsumerListener, mutation: Mutation[_]) {
    //  val handlers = Conf.loadClassesForKey[BinaryLogConsumerErrorHandler]("mypipe.")
  }
}

trait CacheableTableMap extends BinaryLogConsumerTrait {
  protected var tableCache = new TableCache(hostname, port, username, password)

  override protected def handleTableMap(tableMapEvent: TableMapEvent): Table = {
    val table = tableCache.addTableByEvent(tableMapEvent)
    table
  }

  override protected def getTableById(tableId: java.lang.Long): Table =
    tableCache.getTable(tableId).get
}