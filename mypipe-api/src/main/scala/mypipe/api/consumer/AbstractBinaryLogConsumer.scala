package mypipe.api.consumer

import java.util.UUID

import mypipe.api._
import com.fasterxml.uuid.{ EthernetAddress, Generators }

import mypipe.api.data.{ UnknownTable, Table }
import mypipe.api.event._

abstract class AbstractBinaryLogConsumer[BinaryLogEvent, BinaryLogPosition] extends BinaryLogConsumer[BinaryLogEvent, BinaryLogPosition] {

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
    stop()
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
  }

  protected def handleDisconnect() {
    updateBinaryLogPosition()
  }

  def position: Option[BinaryLogPosition] = binLogPos
}
