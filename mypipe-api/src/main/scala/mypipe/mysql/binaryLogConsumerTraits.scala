package mypipe.mysql

import com.typesafe.config.Config
import mypipe.api.{ HostPortUserPass, Conf }

import mypipe.api.consumer.{ BinaryLogConsumerTableFinder, BinaryLogConsumer, BinaryLogConsumerErrorHandler, BinaryLogConsumerListener }
import mypipe.api.data.Table
import mypipe.api.event._
import mypipe.util.Eval
import org.slf4j.LoggerFactory

trait ConnectionSource {
  protected val hostname: String
  protected val port: Int
  protected val username: String
  protected val password: String
}

trait ConfigBasedConnectionSource extends ConnectionSource {
  protected val config: Config
  protected val connectionInfo = HostPortUserPass(config.getString("source"))
  override protected val hostname: String = connectionInfo.host
  override protected val port: Int = connectionInfo.port
  override protected val username: String = connectionInfo.user
  override protected val password: String = connectionInfo.password
}

/** Used when no event skipping behaviour is desired.
 */
trait NoEventSkippingBehaviour {
  this: BinaryLogConsumer[_] ⇒

  protected def skipEvent(e: TableContainingEvent): Boolean = false
}

/** Used alongside the the configuration in order to read in and
 *  compile the code responsible for keeping or skipping events.
 */
// TODO: write a test for this functionality
trait ConfigBasedEventSkippingBehaviour {
  this: BinaryLogConsumer[_] ⇒

  val includeEventCond = Conf.INCLUDE_EVENT_CONDITION

  val skipFn: (String, String) ⇒ Boolean =
    if (includeEventCond.isDefined)
      Eval(s"""{ (db: String, table: String) => { ! ( ${includeEventCond.get} ) } }""")
    else
      (_, _) ⇒ false

  protected def skipEvent(e: TableContainingEvent): Boolean = {
    skipFn(e.table.db, e.table.name)
  }
}

trait ConfigBasedErrorHandlingBehaviour[BinaryLogEvent] extends BinaryLogConsumerErrorHandler[BinaryLogEvent] {

  val handler = Conf.loadClassesForKey[BinaryLogConsumerErrorHandler[BinaryLogEvent]]("mypipe.error.handler")
    .headOption
    .map(_._2.map(_.newInstance()))
    .getOrElse(None)

  def handleEventError(event: Option[Event], binaryLogEvent: BinaryLogEvent): Boolean =
    handler.exists(_.handleEventError(event, binaryLogEvent))

  def handleMutationError(listeners: List[BinaryLogConsumerListener[BinaryLogEvent]], listener: BinaryLogConsumerListener[BinaryLogEvent])(mutation: Mutation): Boolean =
    handler.exists(_.handleMutationError(listeners, listener)(mutation))

  def handleMutationsError(listeners: List[BinaryLogConsumerListener[BinaryLogEvent]], listener: BinaryLogConsumerListener[BinaryLogEvent])(mutations: Seq[Mutation]): Boolean =
    handler.exists(_.handleMutationsError(listeners, listener)(mutations))

  def handleTableMapError(listeners: List[BinaryLogConsumerListener[BinaryLogEvent]], listener: BinaryLogConsumerListener[BinaryLogEvent])(table: Table, event: TableMapEvent): Boolean =
    handler.exists(_.handleTableMapError(listeners, listener)(table, event))

  def handleAlterError(listeners: List[BinaryLogConsumerListener[BinaryLogEvent]], listener: BinaryLogConsumerListener[BinaryLogEvent])(table: Table, event: AlterEvent): Boolean =
    handler.exists(_.handleAlterError(listeners, listener)(table, event))

  def handleCommitError(mutationList: List[Mutation], faultyMutation: Mutation): Boolean =
    handler.exists(_.handleCommitError(mutationList, faultyMutation))

  def handleEventDecodeError(binaryLogEvent: BinaryLogEvent): Boolean =
    handler.exists(_.handleEventDecodeError(binaryLogEvent))

  def handleEmptyCommitError(queryList: List[QueryEvent]): Boolean =
    handler.exists(_.handleEmptyCommitError(queryList))
}

class ConfigBasedErrorHandler[BinaryLogEvent] extends BinaryLogConsumerErrorHandler[BinaryLogEvent] {
  private val log = LoggerFactory.getLogger(getClass)

  private val quitOnEventHandlerFailure = Conf.QUIT_ON_EVENT_HANDLER_FAILURE
  private val quitOnEventDecodeFailure = Conf.QUIT_ON_EVENT_DECODE_FAILURE
  private val quitOnEmptyMutationCommitFailure = Conf.QUIT_ON_EMPTY_MUTATION_COMMIT_FAILURE
  private val quitOnEventListenerFailure = Conf.QUIT_ON_LISTENER_FAILURE

  override def handleEventError(event: Option[Event], binaryLogEvent: BinaryLogEvent): Boolean = {
    log.error("Could not handle event {} from raw event {}", event, binaryLogEvent)
    !quitOnEventHandlerFailure
  }

  override def handleMutationError(listeners: List[BinaryLogConsumerListener[BinaryLogEvent]], listener: BinaryLogConsumerListener[BinaryLogEvent])(mutation: Mutation): Boolean = {
    log.error("Could not handle mutation {} from listener {}", mutation.asInstanceOf[Any], listener)
    !quitOnEventListenerFailure
  }

  override def handleMutationsError(listeners: List[BinaryLogConsumerListener[BinaryLogEvent]], listener: BinaryLogConsumerListener[BinaryLogEvent])(mutations: Seq[Mutation]): Boolean = {
    log.error("Could not handle {} mutation(s) from listener {}", mutations.length, listener)
    !quitOnEventListenerFailure
  }

  override def handleTableMapError(listeners: List[BinaryLogConsumerListener[BinaryLogEvent]], listener: BinaryLogConsumerListener[BinaryLogEvent])(table: Table, event: TableMapEvent): Boolean = {
    log.error("Could not handle table map event {} for table from listener {}", table, event, listener)
    !quitOnEventListenerFailure
  }

  override def handleAlterError(listeners: List[BinaryLogConsumerListener[BinaryLogEvent]], listener: BinaryLogConsumerListener[BinaryLogEvent])(table: Table, event: AlterEvent): Boolean = {
    log.error("Could not handle alter event {} for table {} from listener {}", table, event, listener)
    !quitOnEventListenerFailure
  }

  override def handleCommitError(mutationList: List[Mutation], faultyMutation: Mutation): Boolean = {
    log.error("Could not handle commit due to faulty mutation {} for mutations {}", faultyMutation.asInstanceOf[Any], mutationList)
    !quitOnEventHandlerFailure
  }

  override def handleEmptyCommitError(queryList: List[QueryEvent]): Boolean = {
    val l: (String, Any) ⇒ Unit = if (quitOnEmptyMutationCommitFailure) log.error else log.debug
    l("Could not handle commit due to empty mutation list, missed queries: {}", queryList)
    !quitOnEmptyMutationCommitFailure
  }

  override def handleEventDecodeError(binaryLogEvent: BinaryLogEvent): Boolean = {
    log.trace("Event could not be decoded {}", binaryLogEvent)
    !quitOnEventDecodeFailure
  }
}

trait CacheableTableMapBehaviour extends BinaryLogConsumerTableFinder with ConnectionSource {

  protected var tableCache = new TableCache(hostname, port, username, password)

  override protected def findTable(tableMapEvent: TableMapEvent): Option[Table] = {
    val table = tableCache.addTableByEvent(tableMapEvent)
    Some(table)
  }

  override protected def findTable(tableId: java.lang.Long): Option[Table] =
    tableCache.getTable(tableId)

  override protected def findTable(database: String, table: String): Option[Table] = {
    tableCache.refreshTable(database, table)
  }
}
