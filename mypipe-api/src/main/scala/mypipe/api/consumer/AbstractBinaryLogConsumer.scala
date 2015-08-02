package mypipe.api.consumer

import java.util.UUID

import mypipe.api._
import com.fasterxml.uuid.{ EthernetAddress, Generators }

import mypipe.api.data.{ UnknownTable, Table }
import mypipe.api.event._
import org.slf4j.LoggerFactory

trait BinaryLog[BinaryLogEvent, BinaryLogPosition]

/** Handle BinaryLogConsumer errors. Errors are handled as such:
 *  The first handler deals with event decoding errors.
 *  The second layer of handlers deal with specific event errors, for example: mutation, alter, table map, commit
 *  The third and final layer is the global error handler.
 *
 *  If the first layer or second layer are invoked and they return true, the next event will be consumed and the
 *  global error handler is not called.
 *
 *  If the first layer or second layer are invoked and they return false, then the third layer (global error handler)
 *  is invoked, otherwise, processing of the next event continues.
 *
 *  @tparam BinaryLogEvent binary log event type
 *  @tparam BinaryLogPosition binary log position type
 */
trait BinaryLogConsumerErrorHandler[BinaryLogEvent, BinaryLogPosition] extends BinaryLog[BinaryLogPosition, BinaryLogEvent] {
  def handleEventError(event: Option[Event], binaryLogEvent: BinaryLogEvent): Boolean
  def handleMutationError(listeners: List[BinaryLogConsumerListener[BinaryLogEvent, BinaryLogPosition]], listener: BinaryLogConsumerListener[BinaryLogEvent, BinaryLogPosition])(mutation: Mutation): Boolean
  def handleMutationsError(listeners: List[BinaryLogConsumerListener[BinaryLogEvent, BinaryLogPosition]], listener: BinaryLogConsumerListener[BinaryLogEvent, BinaryLogPosition])(mutations: Seq[Mutation]): Boolean
  def handleTableMapError(listeners: List[BinaryLogConsumerListener[BinaryLogEvent, BinaryLogPosition]], listener: BinaryLogConsumerListener[BinaryLogEvent, BinaryLogPosition])(table: Table, event: TableMapEvent): Boolean
  def handleAlterError(listeners: List[BinaryLogConsumerListener[BinaryLogEvent, BinaryLogPosition]], listener: BinaryLogConsumerListener[BinaryLogEvent, BinaryLogPosition])(table: Table, event: AlterEvent): Boolean
  def handleCommitError(mutationList: List[Mutation], faultyMutation: Mutation): Boolean
  def handleEmptyCommitError(queryList: List[QueryEvent]): Boolean
  def handleEventDecodeError(binaryLogEvent: BinaryLogEvent): Boolean
}

/** Defines what a log consumer should support for mypipe to use it.
 */
trait BinaryLogConsumer[BinaryLogEvent, BinaryLogPosition] extends BinaryLogConsumerErrorHandler[BinaryLogEvent, BinaryLogPosition] {

  /** Given a third-party BinLogEvent, this method decodes it to an
   *  mypipe specific Event type if it recognizes it.
   *  @param binaryLogEvent third-party BinLogEvent to decode
   *  @return the decoded Event or None
   */
  protected def decodeEvent(binaryLogEvent: BinaryLogEvent): Option[Event]

  /** Given a database and table name, returns a valid Table instance
   *  if possible.
   *  @param database the database name
   *  @param table the table name
   *  @return a Table instance or None if not possible
   */
  protected def findTable(database: String, table: String): Option[Table]

  /** Given an TableMapEvent event, returns a valid Table instance
   *  if possible.
   *  @param event the TableMapEvent who's database, table name, and table ID will be used to build the Table
   *  @return a Table instance or None if not possible
   */
  protected def findTable(event: TableMapEvent): Option[Table]

  /** Given an table ID, returns a valid Table instance
   *  if possible.
   *  @param tableId the table ID to find a Table for
   *  @return a Table instance or None if not possible
   */
  protected def findTable(tableId: java.lang.Long): Option[Table]

  /** Disconnects the consumer from it's source.
   */
  protected def disconnect(): Unit

  /** Whether or not to skip the given event. If this method returns true
   *  then the consumer does not process this event and continues to the next one
   *  as if it were processed successfully.
   *
   *  @param e the event to potentially skip
   *  @return true / false to skip or not to skip the event
   */
  protected def skipEvent(e: TableContainingEvent): Boolean

  /** Gets the consumer's current position in the binary log.
   *  @return current BinLogPos
   */
  def getBinaryLogPosition: Option[BinaryLogPosition]

  /** Gets this consumer's unique ID.
   *  @return Unique ID as a string.
   */
  def id: String
}

abstract class AbstractBinaryLogConsumer[BinaryLogEvent, BinaryLogPosition] extends BinaryLogConsumer[BinaryLogEvent, BinaryLogPosition] {

  protected val log = LoggerFactory.getLogger(getClass)

  private val listeners = collection.mutable.Set[BinaryLogConsumerListener[BinaryLogEvent, BinaryLogPosition]]()
  private val groupEventsByTx = Conf.GROUP_EVENTS_BY_TX
  private val groupMutationsByTx = Conf.GROUP_MUTATIONS_BY_TX
  private val txQueue = new scala.collection.mutable.ListBuffer[Mutation]
  private val txQueries = new scala.collection.mutable.ListBuffer[QueryEvent]
  private val uuidGen = Generators.timeBasedGenerator(EthernetAddress.fromInterface())

  private var curTxid: Option[UUID] = None
  private var transactionInProgress = false
  private var binLogPos: Option[BinaryLogPosition] = None

  protected def handleEvent(binaryLogEvent: BinaryLogEvent): Unit = {
    val event = decodeEvent(binaryLogEvent)

    val success = event match {

      case Some(e) ⇒ e match {
        case e: TableContainingEvent if skipEvent(e) ⇒ true
        case e: AlterEvent                           ⇒ handleAlter(e)
        case e: BeginEvent if groupEventsByTx        ⇒ handleBegin(e)
        case e: CommitEvent if groupEventsByTx       ⇒ handleCommit(e)
        case e: RollbackEvent if groupEventsByTx     ⇒ handleRollback(e)
        case e: TableMapEvent                        ⇒ handleTableMap(e)
        case e: XidEvent                             ⇒ handleXid(e)
        case e: Mutation                             ⇒ handleMutation(e)
        case e: UnknownQueryEvent                    ⇒ handleUnknownQueryEvent(e)
        case e: UnknownEvent                         ⇒ handleUnknownEvent(e)
      }
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

  private def handleUnknownEvent(e: UnknownEvent): Boolean = {
    log.debug("Could not process unknown event: {}", e)
    // we move on if the event is unknown to us
    true
  }

  private def handleUnknownQueryEvent(e: UnknownQueryEvent): Boolean = {
    if (transactionInProgress) {
      txQueries += e
    }

    log.debug("Could not process unknown query event: {}", e)
    // we move on if the event is unknown to us
    true
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

    processList[BinaryLogConsumerListener[BinaryLogEvent, BinaryLogPosition]](
      list = listeners.toList,
      listOp = _.onMutation(this, mutation),
      onError = handleMutationError(_, _)(mutation))
  }

  private def handleTableMap(event: TableMapEvent): Boolean = {

    val success = findTable(event).exists(table ⇒ {
      processList[BinaryLogConsumerListener[BinaryLogEvent, BinaryLogPosition]](
        list = listeners.toList,
        listOp = _.onTableMap(this, table),
        onError = handleTableMapError(_, _)(table, event))
    })

    success && updateBinaryLogPosition()
  }

  private def handleAlter(event: AlterEvent): Boolean = {

    val success = event.table match {
      case u: UnknownTable ⇒
        // if we don't find the table we continue, which means downstream handlers don't get called
        log.warn(s"Encountered an alter event but could not find a corresponding Table for it, ignoring and continuing: $event")
        true
      case table: Table ⇒
        processList[BinaryLogConsumerListener[BinaryLogEvent, BinaryLogPosition]](
          list = listeners.toList,
          listOp = _.onTableAlter(this, event),
          onError = handleAlterError(_, _)(table, event))
    }

    success && updateBinaryLogPosition()
  }

  private def handleBegin(event: BeginEvent): Boolean = {
    log.debug("Handling begin event {}", event)
    transactionInProgress = true
    curTxid = Some(uuidGen.generate())
    true
  }

  private def handleRollback(event: RollbackEvent): Boolean = {
    clearTxState()
    updateBinaryLogPosition()
  }

  private def handleCommit(event: CommitEvent): Boolean = {
    log.debug("Handling commit event {}", event)
    commit() && updateBinaryLogPosition()
  }

  private def handleXid(event: XidEvent): Boolean = {
    commit() && updateBinaryLogPosition()
  }

  private def clearTxState() {
    txQueue.clear()
    txQueries.clear()
    transactionInProgress = false
    curTxid = None
  }

  private def commit(): Boolean = {
    if (txQueue.nonEmpty) {
      val success =
        if (groupMutationsByTx) {
          val mutations = txQueue.toList
          processList[BinaryLogConsumerListener[BinaryLogEvent, BinaryLogPosition]](
            list = listeners.toList,
            listOp = _.onMutation(this, mutations),
            onError = handleMutationsError(_, _)(mutations))
        } else {
          processList[Mutation](
            list = txQueue.toList,
            listOp = _handleMutation,
            onError = handleCommitError)
        }

      clearTxState()
      success
    } else {
      val ret = handleEmptyCommitError(txQueries.toList)
      clearTxState()
      ret
    }
  }

  // TODO: move this to a util
  def processList[T](list: List[T],
                     listOp: (T) ⇒ Boolean,
                     onError: (List[T], T) ⇒ Boolean): Boolean = {

    list.forall(item ⇒ {
      val res = try { listOp(item) } catch {
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
    binLogPos.isDefined
  }

  protected def handleConnect() {
    updateBinaryLogPosition()
    listeners foreach (l ⇒ l.onConnect(this))
  }

  protected def handleDisconnect() {
    updateBinaryLogPosition()
    listeners foreach (l ⇒ l.onDisconnect(this))
  }

  def registerListener(listener: BinaryLogConsumerListener[BinaryLogEvent, BinaryLogPosition]) {
    listeners += listener
  }

  def position: Option[BinaryLogPosition] = binLogPos
}
