package mypipe.mysql

import com.github.shyiko.mysql.binlog.BinaryLogClient.{ LifecycleListener, EventListener }
import com.github.shyiko.mysql.binlog.event.{ Event ⇒ MEvent, _ }
import mypipe.api.consumer.AbstractBinaryLogConsumer
import mypipe.api.data.{ Column, Table, Row }
import mypipe.api.event.Event
import mypipe.api.event._

import com.github.shyiko.mysql.binlog.BinaryLogClient
import com.github.shyiko.mysql.binlog.event.EventType._

import scala.collection.JavaConverters._
import scala.collection.immutable.ListMap

abstract class AbstractMySQLBinaryLogConsumer(
  protected val hostname: String,
  protected val port: Int,
  protected val username: String,
  protected val password: String)
    extends AbstractBinaryLogConsumer[MEvent, BinaryLogFilePosition] {

  protected val client = new BinaryLogClient(hostname, port, username, password)

  client.setServerId(MySQLServerId.next)

  client.registerEventListener(new EventListener() {
    override def onEvent(event: MEvent) {
      handleEvent(event)
    }
  })

  client.registerLifecycleListener(new LifecycleListener {
    override def onDisconnect(client: BinaryLogClient): Unit = {
      handleDisconnect()
    }

    override def onConnect(client: BinaryLogClient) {
      handleConnect()
    }

    override def onEventDeserializationFailure(client: BinaryLogClient, ex: Exception) {}
    override def onCommunicationFailure(client: BinaryLogClient, ex: Exception) {}
  })

  def setBinaryLogPosition(binlogFileAndPos: BinaryLogFilePosition): Unit = {
    if (binlogFileAndPos != BinaryLogFilePosition.current) {
      log.info(s"Resuming binlog consumption from file=${binlogFileAndPos.filename} pos=${binlogFileAndPos.pos} for $hostname:$port")
      client.setBinlogFilename(binlogFileAndPos.filename)
      client.setBinlogPosition(binlogFileAndPos.pos)
    } else {
      log.info(s"Using current master binlog position for consuming from $hostname:$port")
    }
  }

  override def id: String = {
    s"$hostname-$port"
  }

  override def getBinaryLogPosition: Option[BinaryLogFilePosition] = {
    Some(BinaryLogFilePosition(client.getBinlogFilename, client.getBinlogPosition))
  }

  override protected def handleError(event: MEvent): Unit = {
    // FIXME: we need better error handling
    client.disconnect()
  }

  override protected def decodeEvent(event: MEvent): Option[Event] = {
    event.getHeader[EventHeader].getEventType match {
      case TABLE_MAP                     ⇒ decodeTableMapEvent(event)
      case QUERY                         ⇒ decodeQueryEvent(event)
      case XID                           ⇒ decodeXidEvent(event)
      case e: EventType if isMutation(e) ⇒ decodeMutationEvent(event)
      case _                             ⇒ Some(UnknownEvent())
    }
  }

  private def decodeTableMapEvent(event: MEvent): Option[TableMapEvent] = {
    val tableMapEventData = event.getData[TableMapEventData]
    Some(TableMapEvent(
      tableMapEventData.getTableId,
      tableMapEventData.getTable,
      tableMapEventData.getDatabase,
      tableMapEventData.getColumnTypes))
  }

  protected def decodeQueryEvent(event: MEvent): Option[QueryEvent] = {
    val queryEventData = event.getData[QueryEventData]
    val query = queryEventData.getSql

    query.toLowerCase match {
      case q if q.indexOf("begin") == 0 ⇒
        Some(BeginEvent(queryEventData.getDatabase, queryEventData.getSql))
      case q if q.indexOf("commit") == 0 ⇒
        Some(CommitEvent(queryEventData.getDatabase, queryEventData.getSql))
      case q if q.indexOf("rollback") == 0 ⇒
        Some(RollbackEvent(queryEventData.getDatabase, queryEventData.getSql))
      case q if q.indexOf("alter") == 0 ⇒
        Some(AlterEvent(queryEventData.getDatabase, queryEventData.getSql))
      case q ⇒
        log.trace("ignoring query={}", q)
        Some(UnknownEvent(queryEventData.getDatabase, queryEventData.getSql))
    }
  }

  protected def decodeXidEvent(event: MEvent): Option[XidEvent] = {
    val eventData = event.getData[XidEventData]
    Some(XidEvent(eventData.getXid))
  }

  protected def decodeMutationEvent(event: MEvent): Option[Mutation] = {
    Some(createMutation(event))
  }

  override def toString: String = s"$hostname:$port"

  def connect(): Unit = client.connect()

  def disconnect(): Unit = client.disconnect()

  protected def isMutation(eventType: EventType): Boolean = {
    EventType.isDelete(eventType) ||
      EventType.isUpdate(eventType) ||
      EventType.isWrite(eventType)
  }

  protected def createMutation(event: MEvent): Mutation = event.getHeader[EventHeader].getEventType match {
    case eventType if EventType.isWrite(eventType) ⇒
      val evData = event.getData[WriteRowsEventData]()
      val table = getTableById(evData.getTableId)
      InsertMutation(table, createRows(table, evData.getRows))

    case eventType if EventType.isUpdate(eventType) ⇒
      val evData = event.getData[UpdateRowsEventData]()
      val table = getTableById(evData.getTableId)
      UpdateMutation(table, createRowsUpdate(table, evData.getRows))

    case eventType if EventType.isDelete(eventType) ⇒
      val evData = event.getData[DeleteRowsEventData]()
      val table = getTableById(evData.getTableId)
      DeleteMutation(table, createRows(table, evData.getRows))
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

  protected def createRowsUpdate(table: Table, evRows: java.util.List[java.util.Map.Entry[Array[java.io.Serializable], Array[java.io.Serializable]]]): List[(Row, Row)] = {
    evRows.asScala.map(evRow ⇒ {

      // zip the names and values from the table's columns and the row's data and
      // create a map that contains column names to Column objects with values
      val old = ListMap.empty[String, Column] ++ table.columns.zip(evRow.getKey).map(c ⇒ c._1.name -> Column(c._1, c._2))
      val cur = ListMap.empty[String, Column] ++ table.columns.zip(evRow.getValue).map(c ⇒ c._1.name -> Column(c._1, c._2))

      (Row(table, old), Row(table, cur))

    }).toList
  }
}
