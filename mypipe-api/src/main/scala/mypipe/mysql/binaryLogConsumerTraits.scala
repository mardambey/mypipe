package mypipe.mysql

import mypipe.api.Conf
import mypipe.api.consumer.{ BinaryLogConsumer, BinaryLogConsumerListener }
import mypipe.api.data.Table
import mypipe.api.event.{ Event, Mutation, AlterEvent, TableMapEvent }
import org.slf4j.LoggerFactory

trait ConfigBasedErrorHandlingBehaviour extends BinaryLogConsumer {

  private val log = LoggerFactory.getLogger(getClass)

  private val quitOnEventHandlerFailure = Conf.QUIT_ON_EVENT_HANDLER_FAILURE
  private val quitOnEventDecodeFailure = Conf.QUIT_ON_EVENT_DECODE_FAILURE
  private val quitOnEventListenerFailure = Conf.QUIT_ON_LISTENER_FAILURE
  //  val handlers = Conf.loadClassesForKey[BinaryLogConsumerErrorHandler]("mypipe.")

  protected def handleEventError(event: Option[Event], binaryLogEvent: BinLogEvent): Boolean = {
    log.error("Could not handle event {} from raw event {}", event, binaryLogEvent)
    !quitOnEventHandlerFailure
  }

  protected def handleMutationError(listeners: List[BinaryLogConsumerListener], listener: BinaryLogConsumerListener)(mutation: Mutation): Boolean = {
    log.error("Could not handle mutation {} from listener {}", mutation.asInstanceOf[Any], listener)
    !quitOnEventListenerFailure
  }

  protected def handleTableMapError(listeners: List[BinaryLogConsumerListener], listener: BinaryLogConsumerListener)(table: Table, event: TableMapEvent): Boolean = {
    log.error("Could not handle table map event {} for table from listener {}", table, event, listener)
    !quitOnEventListenerFailure
  }

  protected def handleAlterError(listeners: List[BinaryLogConsumerListener], listener: BinaryLogConsumerListener)(table: Table, event: AlterEvent): Boolean = {
    log.error("Could not handle alter event {} for table {} from listener {}", table, event, listener)
    !quitOnEventListenerFailure
  }

  protected def handleCommitError(mutationList: List[Mutation], faultyMutation: Mutation): Boolean = {
    log.error("Could not handle commit due to faulty mutation {} for mutations {}", faultyMutation.asInstanceOf[Any], mutationList)
    !quitOnEventHandlerFailure
  }

  protected def handleEventDecodeError(binaryLogEvent: BinLogEvent): Boolean = {
    log.trace("Event could not be decoded {}", binaryLogEvent)
    !quitOnEventDecodeFailure
  }
}

trait CacheableTableMapBehaviour extends AbstractMySQLBinaryLogConsumer {

  protected var tableCache = new TableCache(hostname, port, username, password)

  protected def findTable(tableMapEvent: TableMapEvent): Option[Table] = {
    val table = tableCache.addTableByEvent(tableMapEvent)
    Some(table)
  }

  protected def getTableById(tableId: java.lang.Long): Option[Table] =
    tableCache.getTable(tableId)

  protected def findTable(event: AlterEvent): Option[Table] = {
    // FIXME: this sucks and needs to be parsed properly
    val tableName = {
      val t = event.sql.split(" ")(2)
      // account for db.table
      if (t.contains(".")) t.split("""\.""")(1)
      else t
    }

    tableCache.refreshTable(event.database, tableName)
  }
}
