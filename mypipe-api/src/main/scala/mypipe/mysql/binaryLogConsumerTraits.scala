package mypipe.mysql

import mypipe.api.Conf
import mypipe.api.consumer.{ BinaryLogConsumerErrorHandler, BinaryLogConsumerListener }
import mypipe.api.data.Table
import mypipe.api.event._
import org.slf4j.LoggerFactory

trait ConfigBasedErrorHandlingBehaviour[BinaryLogEvent, BinaryLogPosition] extends BinaryLogConsumerErrorHandler[BinaryLogEvent, BinaryLogPosition] {

  val handler = Conf.loadClassesForKey[BinaryLogConsumerErrorHandler[BinaryLogEvent, BinaryLogPosition]]("mypipe.error.handler")
    .headOption
    .map(_._2.newInstance())

  def handleEventError(event: Option[Event], binaryLogEvent: BinaryLogEvent): Boolean =
    handler.exists(_.handleEventError(event, binaryLogEvent))

  def handleMutationError(listeners: List[BinaryLogConsumerListener[BinaryLogEvent, BinaryLogPosition]], listener: BinaryLogConsumerListener[BinaryLogEvent, BinaryLogPosition])(mutation: Mutation): Boolean =
    handler.exists(_.handleMutationError(listeners, listener)(mutation))

  def handleTableMapError(listeners: List[BinaryLogConsumerListener[BinaryLogEvent, BinaryLogPosition]], listener: BinaryLogConsumerListener[BinaryLogEvent, BinaryLogPosition])(table: Table, event: TableMapEvent): Boolean =
    handler.exists(_.handleTableMapError(listeners, listener)(table, event))

  def handleAlterError(listeners: List[BinaryLogConsumerListener[BinaryLogEvent, BinaryLogPosition]], listener: BinaryLogConsumerListener[BinaryLogEvent, BinaryLogPosition])(table: Table, event: AlterEvent): Boolean =
    handler.exists(_.handleAlterError(listeners, listener)(table, event))

  def handleCommitError(mutationList: List[Mutation], faultyMutation: Mutation): Boolean =
    handler.exists(_.handleCommitError(mutationList, faultyMutation))

  def handleEventDecodeError(binaryLogEvent: BinaryLogEvent): Boolean =
    handler.exists(_.handleEventDecodeError(binaryLogEvent))

  def handleEmptyCommitError(queryList: List[QueryEvent]): Boolean =
    handler.exists(_.handleEmptyCommitError(queryList))
}

class ConfigBasedErrorHandler[BinaryLogEvent, BinaryLogPosition] extends BinaryLogConsumerErrorHandler[BinaryLogEvent, BinaryLogPosition] {
  private val log = LoggerFactory.getLogger(getClass)

  private val quitOnEventHandlerFailure = Conf.QUIT_ON_EVENT_HANDLER_FAILURE
  private val quitOnEventDecodeFailure = Conf.QUIT_ON_EVENT_DECODE_FAILURE
  private val quitOnEmptyMutationCommitFailure = Conf.QUIT_ON_EMPTY_MUTATION_COMMIT_FAILURE
  private val quitOnEventListenerFailure = Conf.QUIT_ON_LISTENER_FAILURE

  override def handleEventError(event: Option[Event], binaryLogEvent: BinaryLogEvent): Boolean = {
    log.error("Could not handle event {} from raw event {}", event, binaryLogEvent)
    !quitOnEventHandlerFailure
  }

  override def handleMutationError(listeners: List[BinaryLogConsumerListener[BinaryLogEvent, BinaryLogPosition]], listener: BinaryLogConsumerListener[BinaryLogEvent, BinaryLogPosition])(mutation: Mutation): Boolean = {
    log.error("Could not handle mutation {} from listener {}", mutation.asInstanceOf[Any], listener)
    !quitOnEventListenerFailure
  }

  override def handleTableMapError(listeners: List[BinaryLogConsumerListener[BinaryLogEvent, BinaryLogPosition]], listener: BinaryLogConsumerListener[BinaryLogEvent, BinaryLogPosition])(table: Table, event: TableMapEvent): Boolean = {
    log.error("Could not handle table map event {} for table from listener {}", table, event, listener)
    !quitOnEventListenerFailure
  }

  override def handleAlterError(listeners: List[BinaryLogConsumerListener[BinaryLogEvent, BinaryLogPosition]], listener: BinaryLogConsumerListener[BinaryLogEvent, BinaryLogPosition])(table: Table, event: AlterEvent): Boolean = {
    log.error("Could not handle alter event {} for table {} from listener {}", table, event, listener)
    !quitOnEventListenerFailure
  }

  override def handleCommitError(mutationList: List[Mutation], faultyMutation: Mutation): Boolean = {
    log.error("Could not handle commit due to faulty mutation {} for mutations {}", faultyMutation.asInstanceOf[Any], mutationList)
    !quitOnEventHandlerFailure
  }

  override def handleEmptyCommitError(queryList: List[QueryEvent]): Boolean = {
    log.error("Could not handle commit due to empty mutation list, missed queries: {}", queryList)
    !quitOnEmptyMutationCommitFailure
  }

  override def handleEventDecodeError(binaryLogEvent: BinaryLogEvent): Boolean = {
    log.trace("Event could not be decoded {}", binaryLogEvent)
    !quitOnEventDecodeFailure
  }
}

trait CacheableTableMapBehaviour extends AbstractMySQLBinaryLogConsumer {

  protected var tableCache = new TableCache(hostname, port, username, password)

  override protected def findTable(tableMapEvent: TableMapEvent): Option[Table] = {
    val table = tableCache.addTableByEvent(tableMapEvent)
    Some(table)
  }

  override protected def findTable(tableId: java.lang.Long): Option[Table] =
    tableCache.getTable(tableId)

  override protected def findTable(event: AlterEvent): Option[Table] = {
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
