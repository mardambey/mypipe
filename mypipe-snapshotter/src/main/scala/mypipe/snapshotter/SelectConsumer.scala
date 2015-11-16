package mypipe.snapshotter

import java.lang.Long

import mypipe.api.consumer.{BinaryLogConsumer, BinaryLogConsumerListener}
import mypipe.api.data.{Column, Row, Table}
import mypipe.api.event._
import mypipe.mysql.BinaryLogFilePosition

import scala.collection.JavaConverters._
import scala.collection.immutable.ListMap

case class SelectEvent(database: String, table: String, rows: Seq[Seq[Any]])

class SelectConsumer extends BinaryLogConsumer[SelectEvent, BinaryLogFilePosition] {
  /** Given a third-party BinLogEvent, this method decodes it to an
   *  mypipe specific Event type if it recognizes it.
   *  @param event the event to decode
   *  @return the decoded Event or None
   */
  override protected def decodeEvent(event: SelectEvent): Option[Event] = {
    val table = findTable(event.database, event.table)
    val rowData = event.rows.map(_.map(_.asInstanceOf[java.io.Serializable]).toArray).toList.asJava
    val rows = createRows(table.get, rowData)
    Some(InsertMutation(table.get, rows))
  }

  protected def createRows(table: Table, evRows: java.util.List[Array[java.io.Serializable]]): List[Row] = {
    evRows.asScala.map(evRow ⇒ {

      // zip the names and values from the table's columns and the row's data and
      // create a map that contains column names to Column objects with values
      val cols = table.columns.zip(evRow).map(c ⇒ c._1.name -> Column(c._1, c._2))
      val columns = ListMap.empty[String, Column] ++ cols.toArray

      Row(table, columns)

    }).toList
  }

  /** Given a database and table name, returns a valid Table instance
   *  if possible.
   *  @param database the database name
   *  @param table the table name
   *  @return a Table instance or None if not possible
   */
  override protected def findTable(database: String, table: String): Option[Table] = ???

  /** Given an TableMapEvent event, returns a valid Table instance
   *  if possible.
   *  @param event the TableMapEvent who's database, table name, and table ID will be used to build the Table
   *  @return a Table instance or None if not possible
   */
  override protected def findTable(event: TableMapEvent): Option[Table] = ???

  /** Given an table ID, returns a valid Table instance
   *  if possible.
   *  @param tableId the table ID to find a Table for
   *  @return a Table instance or None if not possible
   */
  override protected def findTable(tableId: Long): Option[Table] = ???

  /** Disconnects the consumer from it's source.
   */
  override protected def disconnect(): Unit = ???

  /** Gets the consumer's current position in the binary log.
   *  @return current BinLogPos
   */
  override def getBinaryLogPosition: Option[BinaryLogFilePosition] = ???

  /** Whether or not to skip the given event. If this method returns true
   *  then the consumer does not process this event and continues to the next one
   *  as if it were processed successfully.
   *
   *  @param e the event to potentially skip
   *  @return true / false to skip or not to skip the event
   */
  override protected def skipEvent(e: TableContainingEvent): Boolean = ???

  /** Gets this consumer's unique ID.
   *  @return Unique ID as a string.
   */
  override def id: String = ???

  override def handleMutationsError(listeners: List[BinaryLogConsumerListener[SelectEvent, BinaryLogFilePosition]], listener: BinaryLogConsumerListener[SelectEvent, BinaryLogFilePosition])(mutations: Seq[Mutation]): Boolean = ???

  override def handleCommitError(mutationList: List[Mutation], faultyMutation: Mutation): Boolean = ???

  override def handleTableMapError(listeners: List[BinaryLogConsumerListener[SelectEvent, BinaryLogFilePosition]], listener: BinaryLogConsumerListener[SelectEvent, BinaryLogFilePosition])(table: Table, event: TableMapEvent): Boolean = ???

  override def handleEventDecodeError(binaryLogEvent: SelectEvent): Boolean = ???

  override def handleEmptyCommitError(queryList: List[QueryEvent]): Boolean = ???

  override def handleMutationError(listeners: List[BinaryLogConsumerListener[SelectEvent, BinaryLogFilePosition]], listener: BinaryLogConsumerListener[SelectEvent, BinaryLogFilePosition])(mutation: Mutation): Boolean = ???

  override def handleAlterError(listeners: List[BinaryLogConsumerListener[SelectEvent, BinaryLogFilePosition]], listener: BinaryLogConsumerListener[SelectEvent, BinaryLogFilePosition])(table: Table, event: AlterEvent): Boolean = ???

  override def handleEventError(event: Option[Event], binaryLogEvent: SelectEvent): Boolean = ???
}
