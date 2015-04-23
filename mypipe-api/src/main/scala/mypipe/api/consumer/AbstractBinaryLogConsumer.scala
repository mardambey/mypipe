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
  protected def findTable(event: TableMapEvent): Table
  protected def getTableById(tableId: java.lang.Long): Table
  protected def handleError(binaryLogEvent: BinLogEvent)
  protected def handleError(listener: BinaryLogConsumerListener, mutation: Mutation)

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

  // TODO: make this configurable
  private val quitOnEventHandleFailure = true

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

      case Some(e: BeginEvent) if groupEventsByTx    ⇒ handleBegin(e)
      case Some(e: CommitEvent) if groupEventsByTx   ⇒ handleCommit(e)
      case Some(e: RollbackEvent) if groupEventsByTx ⇒ handleRollback(e)
      case Some(e: AlterEvent)                       ⇒ handleAlter(e)
      case Some(e: TableMapEvent)                    ⇒ handleTableMap(e)
      case Some(e: XidEvent)                         ⇒ handleXid(e)
      case Some(e: Mutation)                         ⇒ handleMutation(e)
      case Some(e: UnknownEvent)                     ⇒ true // we move on if the event is unknown to us
      case _                                         ⇒ handleEventDecodeError(binaryLogEvent)
    }

    if (!success) {

      log.error(s"Failed to process event $event from $binaryLogEvent")

      if (quitOnEventHandleFailure) {
        log.error("Failure encountered and asked to quit on event handler failure, disconnecting consumer {}", id)
        handleError(binaryLogEvent)
      }
    }
  }

  private def handleEventDecodeError(binaryLogEvent: BinaryLogEvent): Boolean = {
    log.trace("Event ignored {}", binaryLogEvent)
    false
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

  private def handleTableMap(event: TableMapEvent): Boolean = {
    val table = findTable(event)
    listeners.foreach(_.onTableMap(this, table))
    updateBinaryLogPosition()
  }

  private def handleAlter(event: AlterEvent): Boolean = {
    val table = findTable(event)
    table.foreach(t ⇒ listeners foreach (l ⇒ l.onTableAlter(this, t)))
    updateBinaryLogPosition()
  }

  private def handleBegin(event: BeginEvent): Boolean = {
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
    commit() && updateBinaryLogPosition()
  }

  private def handleXid(event: XidEvent): Boolean = {
    commit() && updateBinaryLogPosition()
  }

  private def commit(): Boolean = {
    // TODO: take care of _handleMutation's listeners returning false

    if (txQueue.nonEmpty) {
      txQueue.foreach(_handleMutation)
      txQueue.clear()
      transactionInProgress = false
      curTxid = None
    }

    true
  }

  private def updateBinaryLogPosition(): Boolean = {
    binLogPos = getBinaryLogPosition
    binLogPos != None
  }

  protected def handleDisconnect() {
    listeners foreach (l ⇒ l.onDisconnect(this))
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
