package mypipe.api.consumer

import mypipe.api._

import mypipe.api.data.Table
import mypipe.api.event._

/** Created by hisham on 08/12/14.
 */
abstract class AbstractBinaryLogConsumer {

  val hostname: String
  val port: Int
  val username: String
  val password: String

  protected val listeners = collection.mutable.Set[BinaryLogConsumerListener]()
  protected val groupEventsByTx = Conf.GROUP_EVENTS_BY_TX
  protected val txQueue = new scala.collection.mutable.ListBuffer[Mutation[_]]

  protected var transactionInProgress = false

  protected def handleError(listener: BinaryLogConsumerListener, mutation: Mutation[_])

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

  protected def handleTableMap(event: TableMapEvent): Table = {
    val table = findTable(event)
    listeners.foreach(_.onTableMap(this, table))
    table
  }

  protected def handleAlter(event: AlterEvent): Unit = {
    val table = findTable(event)
    table.map(t ⇒ listeners foreach (l ⇒ l.onTableAlter(this, t)))
  }

  protected def handleBegin(event: BeginEvent): Unit = {
    transactionInProgress = true
  }

  protected def handleRollback(event: RollbackEvent): Unit = {
    txQueue.clear()
    transactionInProgress = false
  }

  protected def handleCommit(event: CommitEvent): Unit = {
    commit()
  }

  protected def handleXid(event: XidEvent): Unit = {
    commit()
  }

  private def commit(): Unit = {
    // TODO: take of _handleMutation's listeners returning false
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

  protected def getTableById(tableId: java.lang.Long): Table

  protected def findTable(event: AlterEvent): Option[Table]
  protected def findTable(event: TableMapEvent): Table
}
