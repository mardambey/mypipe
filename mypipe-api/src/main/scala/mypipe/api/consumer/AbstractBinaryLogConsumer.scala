package mypipe.api.consumer

import java.util.UUID

import mypipe.api._
import com.fasterxml.uuid.{ EthernetAddress, Generators }

import mypipe.api.data.Table
import mypipe.api.event._
import org.slf4j.LoggerFactory

/** Defines what a log consumer should support for mypipe to use it.
 */
trait BinaryLogConsumer {
  type BinLogPos
  type BinLogEvent

  protected def decodeEvent(binaryLogEvent: BinLogEvent): Option[Event]
  protected def findTable(event: AlterEvent): Option[Table]
  protected def findTable(event: TableMapEvent): Option[Table]
  protected def getTableById(tableId: java.lang.Long): Option[Table]

  protected def disconnect(): Unit

  /** Gets the log's current position.
   *  @return current BinLogPos
   */
  def getBinaryLogPosition: Option[BinLogPos]

  /** Gets this consumer's unique ID.
   *  @return Unique ID as a string.
   */
  def id: String
}

abstract class AbstractBinaryLogConsumer[BinaryLogEvent, BinaryLogPosition] extends BinaryLogConsumer {

  type BinLogPos = BinaryLogPosition
  type BinLogEvent = BinaryLogEvent

  // abstract functions
  protected def handleEventError(event: Option[Event], binaryLogEvent: BinaryLogEvent): Boolean
  protected def handleMutationError(listeners: List[BinaryLogConsumerListener], listener: BinaryLogConsumerListener)(mutation: Mutation): Boolean
  protected def handleTableMapError(listeners: List[BinaryLogConsumerListener], listener: BinaryLogConsumerListener)(table: Table, event: TableMapEvent): Boolean
  protected def handleAlterError(listeners: List[BinaryLogConsumerListener], listener: BinaryLogConsumerListener)(table: Table, event: AlterEvent): Boolean
  protected def handleCommitError(mutationList: List[Mutation], faultyMutation: Mutation): Boolean
  protected def handleEventDecodeError(binaryLogEvent: BinaryLogEvent): Boolean

  // TODO: make these configurable
  private val listenerFailFast = Conf.LISTENER_FAIL_FAST

  private val listeners = collection.mutable.Set[BinaryLogConsumerListener]()
  private val groupEventsByTx = Conf.GROUP_EVENTS_BY_TX
  private val txQueue = new scala.collection.mutable.ListBuffer[Mutation]
  private val uuidGen = Generators.timeBasedGenerator(EthernetAddress.fromInterface())
  private var curTxid: Option[UUID] = None

  private var transactionInProgress = false
  private var binLogPos: Option[BinaryLogPosition] = None

  protected val log = LoggerFactory.getLogger(getClass)

  protected def handleEvent(binaryLogEvent: BinaryLogEvent): Unit = {
    val event = decodeEvent(binaryLogEvent)

    val success = event match {

      // TODO: each of the following cases that can fail needs it's own error handler
      case Some(e: BeginEvent) if groupEventsByTx    ⇒ handleBegin(e)
      case Some(e: CommitEvent) if groupEventsByTx   ⇒ handleCommit(e)
      case Some(e: RollbackEvent) if groupEventsByTx ⇒ handleRollback(e)
      case Some(e: AlterEvent)                       ⇒ handleAlter(e)
      case Some(e: TableMapEvent)                    ⇒ handleTableMap(e)
      case Some(e: XidEvent)                         ⇒ handleXid(e)
      case Some(e: Mutation)                         ⇒ handleMutation(e)
      // we move on if the event is unknown to us
      case Some(e: UnknownEvent) ⇒
        log.debug("Could not process unknown event: {}", e); true
      // TODO: this is going to be the only error handler accepting the 3rd party event; allow the user to override?
      case None ⇒ handleEventDecodeError(binaryLogEvent)
    }

    if (!success) {
      log.error(s"Consumer $id failed to process event $event from $binaryLogEvent")

      if (!handleEventError(event, binaryLogEvent))
        _disconnect()
    }
  }

  private def _disconnect(): Unit = {
    disconnect()
    listeners foreach (l ⇒ l.onDisconnect(this))
  }

  private def handleMutation(mutation: Mutation): Boolean = {
    if (transactionInProgress) {
      val i = curTxid.get
      txQueue += mutation.txAware(txid = i)
      true
    } else {
      _handleMutation(mutation) && updateBinaryLogPosition()
    }
  }

  private def _handleMutation(mutation: Mutation): Boolean = {

    processList[BinaryLogConsumerListener](
      listeners.toList,
      op = _.onMutation(this, mutation),
      onError = handleMutationError(_, _)(mutation))
  }

  private def handleTableMap(event: TableMapEvent): Boolean = {

    val success = findTable(event).exists(table ⇒ {
      processList[BinaryLogConsumerListener](
        listeners.toList,
        op = _.onTableMap(this, table),
        onError = handleTableMapError(_, _)(table, event))
    })

    success && updateBinaryLogPosition()
  }

  private def handleAlter(event: AlterEvent): Boolean = {

    val success = findTable(event).exists(table ⇒ {
      processList[BinaryLogConsumerListener](
        listeners.toList,
        op = _.onTableAlter(this, table),
        onError = handleAlterError(_, _)(table, event))
    })

    success && updateBinaryLogPosition()
  }

  private def handleBegin(event: BeginEvent): Boolean = {
    log.debug("Handling begin event {}", event)
    transactionInProgress = true
    curTxid = Some(uuidGen.generate())
    true
  }

  private def handleRollback(event: RollbackEvent): Boolean = {
    txQueue.clear()
    transactionInProgress = false
    curTxid = None
    updateBinaryLogPosition()
  }

  private def handleCommit(event: CommitEvent): Boolean = {
    log.debug("Handling commit event {}", event)
    commit() && updateBinaryLogPosition()
  }

  private def handleXid(event: XidEvent): Boolean = {
    commit() && updateBinaryLogPosition()
  }

  private def commit(): Boolean = {
    if (txQueue.nonEmpty) {

      val success = processList[Mutation](txQueue.toList,
        op = _handleMutation,
        onError = handleCommitError)

      txQueue.clear()
      transactionInProgress = false
      curTxid = None
      success
    } else {
      false
    }
  }

  // TODO: move this to a util
  def processList[T](list: List[T],
                     op: (T) ⇒ Boolean,
                     onError: (List[T], T) ⇒ Boolean): Boolean = {

    list.forall(item ⇒ {
      val res = try { op(item) } catch {
        case e: Exception ⇒
          log.error("Unhandled exception while processing list", e)
          onError(list, item)
      }

      if (!res) {
        // fail-fast if the error handler returns false
        onError(list, item)
      } else true

    })
  }

  private def updateBinaryLogPosition(): Boolean = {
    binLogPos = getBinaryLogPosition
    binLogPos != None
  }

  protected def handleConnect() {
    updateBinaryLogPosition()
    listeners foreach (l ⇒ l.onConnect(this))
  }

  def registerListener(listener: BinaryLogConsumerListener) {
    listeners += listener
  }

  def position: Option[BinaryLogPosition] = binLogPos
}
