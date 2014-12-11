package mypipe.api.consumer

import mypipe.api._

import mypipe.api.data.Table
import mypipe.api.event._
import org.slf4j.LoggerFactory

trait BinaryLogConsumer {
  val hostname: String
  val port: Int
  val username: String
  val password: String

  protected def getTableById(tableId: java.lang.Long): Table
  protected def findTable(event: AlterEvent): Option[Table]
  protected def findTable(event: TableMapEvent): Table
  protected def handleError(listener: BinaryLogConsumerListener, mutation: Mutation[_])
}

abstract class AbstractBinaryLogConsumer[BinaryLogEvent] extends BinaryLogConsumer {

  protected def decodeEvent(binaryLogEvent: BinaryLogEvent): Option[Event]
  protected def handleError(binaryLogEvent: BinaryLogEvent)

  // TODO: make this configurable
  protected val quitOnEventHandleFailure = true

  protected val listeners = collection.mutable.Set[BinaryLogConsumerListener]()
  protected val groupEventsByTx = Conf.GROUP_EVENTS_BY_TX
  protected val txQueue = new scala.collection.mutable.ListBuffer[Mutation[_]]

  protected var transactionInProgress = false

  protected val log = LoggerFactory.getLogger(getClass)

  protected def handleEvent(binaryLogEvent: BinaryLogEvent): Unit = {
    val event = decodeEvent(binaryLogEvent)

    val success = event match {

      case Some(e: TableMapEvent) ⇒ handleTableMap(e)
      case Some(e: QueryEvent)    ⇒ handleQueryEvent(e)
      case Some(e: XidEvent)      ⇒ handleXid(e)
      case Some(e: Mutation[_])   ⇒ handleMutation(e)
      case Some(e: UnknownEvent)  ⇒ true // we move on if the event is unknown to us
      case _                      ⇒ handleEventDecodeError(binaryLogEvent)
    }

    if (!success) {

      log.error(s"Failed to process event $event from $binaryLogEvent")

      if (quitOnEventHandleFailure) {
        log.error("Failure encountered and asked to quit on event handler failure, disconnecting from {}:{}", hostname, port)
        handleError(binaryLogEvent)
      }
    }
  }

  protected def handleQueryEvent(queryEvent: QueryEvent): Boolean = {
    queryEvent match {
      case e: BeginEvent if groupEventsByTx    ⇒ handleBegin(e)
      case e: CommitEvent if groupEventsByTx   ⇒ handleCommit(e)
      case e: RollbackEvent if groupEventsByTx ⇒ handleRollback(e)
      case e: AlterEvent                       ⇒ handleAlter(e)
      case q                                   ⇒ log.trace("Ignoring query event {}", queryEvent); true
    }
  }

  protected def handleEventDecodeError(binaryLogEvent: BinaryLogEvent): Boolean = {
    log.trace("Event ignored {}", binaryLogEvent)
    false
  }

  protected def handleMutation(mutation: Mutation[_]): Boolean = {
    if (transactionInProgress) {
      txQueue += mutation
      true
    } else {
      _handleMutation(mutation)
    }
  }

  private def _handleMutation(mutation: Mutation[_]): Boolean = {
    val results = listeners.takeWhile(l ⇒
      try {
        l.onMutation(this, mutation)
      } catch {
        case e: Exception ⇒
          handleError(l, mutation)
          false
      })

    if (results.size == listeners.size) true
    else false
  }

  protected def handleTableMap(event: TableMapEvent): Boolean = {
    val table = findTable(event)
    listeners.foreach(_.onTableMap(this, table))
    true
  }

  protected def handleAlter(event: AlterEvent): Boolean = {
    val table = findTable(event)
    table.map(t ⇒ listeners foreach (l ⇒ l.onTableAlter(this, t)))
    // FIXME: return proper value
    true
  }

  protected def handleBegin(event: BeginEvent): Boolean = {
    transactionInProgress = true
    // FIXME: return proper value
    true
  }

  protected def handleRollback(event: RollbackEvent): Boolean = {
    txQueue.clear()
    transactionInProgress = false
    // FIXME: return proper value
    true
  }

  protected def handleCommit(event: CommitEvent): Boolean = {
    commit()
    // FIXME: return proper value
    true
  }

  protected def handleXid(event: XidEvent): Boolean = {
    commit()
    // FIXME: return proper value
    true
  }

  private def commit(): Unit = {
    // TODO: take care of _handleMutation's listeners returning false
    txQueue.foreach(_handleMutation)
    txQueue.clear()
    transactionInProgress = false
  }

  protected def handleDisconnect() {
    listeners foreach (l ⇒ l.onDisconnect(this))
  }

  protected def handleConnect() {
    listeners foreach (l ⇒ l.onConnect(this))
  }

  def registerListener(listener: BinaryLogConsumerListener) {
    listeners += listener
  }
}
