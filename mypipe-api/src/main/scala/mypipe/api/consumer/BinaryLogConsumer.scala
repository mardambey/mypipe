package mypipe.api.consumer

import mypipe.api.data.Table
import mypipe.api.event._

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

trait BinaryLogConsumerTableFinder {
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
}

/** Defines what a log consumer should support for mypipe to use it.
 */
trait BinaryLogConsumer[BinaryLogEvent, BinaryLogPosition] extends BinaryLogConsumerErrorHandler[BinaryLogEvent, BinaryLogPosition] with BinaryLogConsumerTableFinder {

  /** Set of listeners receiving events from this consumer.
   */
  protected val listeners = collection.mutable.Set[BinaryLogConsumerListener[BinaryLogEvent, BinaryLogPosition]]()

  /** Given a third-party BinLogEvent, this method decodes it to an
   *  mypipe specific Event type if it recognizes it.
   *  @param binaryLogEvent third-party BinLogEvent to decode
   *  @return the decoded Event or None
   */
  protected def decodeEvent(binaryLogEvent: BinaryLogEvent): Option[Event]

  /** Whether or not to skip the given event. If this method returns true
   *  then the consumer does not process this event and continues to the next one
   *  as if it were processed successfully.
   *
   *  @param e the event to potentially skip
   *  @return true / false to skip or not to skip the event
   */
  protected def skipEvent(e: TableContainingEvent): Boolean

  /** Consumer specific startup logic.
   */
  protected def onStart(): Unit

  /** Consumer specific shutdown logic.
   */
  protected def onStop(): Unit

  /** Registers a listener for this consumer's events.
   *  @param listener the BinaryLogConsumerListener to register
   */
  def registerListener(listener: BinaryLogConsumerListener[BinaryLogEvent, BinaryLogPosition]) {
    listeners += listener
  }

  /** Gets the consumer's current position in the binary log.
   *  @return current BinLogPos
   */
  def getBinaryLogPosition: Option[BinaryLogPosition]

  /** Gets this consumer's unique ID.
   *  @return Unique ID as a string.
   */
  def id: String

  /** Starts the consumer.
   */
  final def start(): Unit = {
    listeners foreach (l ⇒ l.onConnect(this))
  }

  /** Stops the consumer.
   */
  final def stop(): Unit = {
    listeners.foreach(_.onDisconnect(this))
  }
}
