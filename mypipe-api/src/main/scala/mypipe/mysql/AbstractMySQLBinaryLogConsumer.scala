package mypipe.mysql

import com.github.shyiko.mysql.binlog.BinaryLogClient.{ LifecycleListener, EventListener }
import com.github.shyiko.mysql.binlog.event.{ Event ⇒ MEvent, _ }
import mypipe.api.consumer.AbstractBinaryLogConsumer
import mypipe.api.data.{ UnknownTable, Column, Table, Row }
import mypipe.api.event.Event
import mypipe.api.event._

import com.github.shyiko.mysql.binlog.BinaryLogClient
import com.github.shyiko.mysql.binlog.event.EventType._

import scala.collection.JavaConverters._
import scala.collection.immutable.ListMap
import scala.compat.Platform
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

abstract class AbstractMySQLBinaryLogConsumer
    extends AbstractBinaryLogConsumer[MEvent, BinaryLogFilePosition]
    with ConnectionSource {

  protected lazy val client = new BinaryLogClient(hostname, port, username, password)

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

    Some(query.toLowerCase match {
      case q if q.indexOf("begin") == 0 ⇒
        BeginEvent(queryEventData.getDatabase, queryEventData.getSql)
      case q if q.indexOf("commit") == 0 ⇒
        CommitEvent(queryEventData.getDatabase, queryEventData.getSql)
      case q if q.indexOf("rollback") == 0 ⇒
        RollbackEvent(queryEventData.getDatabase, queryEventData.getSql)
      case q if q.indexOf("alter") == 0 ⇒
        val databaseName = {
          if (queryEventData.getDatabase.nonEmpty) queryEventData.getDatabase
          else decodeDatabaseFromAlter(queryEventData.getSql)
        }

        val tableName = decodeTableFromAlter(queryEventData.getSql)
        val table = findTable(databaseName, tableName).getOrElse(new UnknownTable(-1.toLong, name = tableName, db = databaseName))
        AlterEvent(table, queryEventData.getSql)
      case q ⇒
        log.debug("Consumer {} ignoring unknown query query={}", Array(id, q): _*)
        UnknownQueryEvent(queryEventData.getDatabase, queryEventData.getSql)
    })
  }

  private def decodeDatabaseFromAlter(sql: String): String = {
    // FIXME: this sucks, parse properly, not "by hand"
    val tokens = sql.split("""\s""")

    if (tokens.size > 3 && tokens(1).toLowerCase == "table" && tokens(2).contains(".")) {
      tokens(2).split("""\.""").head
    } else {
      // we could not find a database name, this is not good
      log.error("Could not find database for query: {}", sql)
      ""
    }
  }

  private def decodeTableFromAlter(sql: String): String = {
    // FIXME: this sucks and needs to be parsed properly
    val t = sql.split(" ")(2)
    // account for db.table
    if (t.contains(".")) t.split("""\.""")(1)
    else t
  }

  protected def decodeXidEvent(event: MEvent): Option[XidEvent] = {
    val eventData = event.getData[XidEventData]
    Some(XidEvent(eventData.getXid))
  }

  protected def decodeMutationEvent(event: MEvent): Option[Mutation] = {
    Some(createMutation(event))
  }

  override def toString: String = s"$hostname:$port"

  override protected def onStart(): Future[Boolean] = {
    Future {
      @volatile var connected = false

      client.setServerId(MySQLServerId.next)
      client.registerEventListener(new EventListener() {
        override def onEvent(event: MEvent) = handleEvent(event)
      })

      client.registerLifecycleListener(new LifecycleListener {
        override def onDisconnect(client: BinaryLogClient) = handleDisconnect()
        override def onConnect(client: BinaryLogClient) = { connected = true; handleConnect() }
        override def onEventDeserializationFailure(client: BinaryLogClient, ex: Exception) {}
        override def onCommunicationFailure(client: BinaryLogClient, ex: Exception) {}
      })

      log.info(s"Connecting client to $client.get:$hostname:$port")

      Future { client.connect() }

      val startTime = Platform.currentTime
      while (Platform.currentTime - startTime < 10000 && !connected) Thread.sleep(10)

      connected
    }
  }

  override protected def onStop(): Unit = client.disconnect()

  protected def isMutation(eventType: EventType): Boolean = {
    EventType.isDelete(eventType) ||
      EventType.isUpdate(eventType) ||
      EventType.isWrite(eventType)
  }

  protected def createMutation(event: MEvent): Mutation = event.getHeader[EventHeader].getEventType match {
    case eventType if EventType.isWrite(eventType) ⇒
      val evData = event.getData[WriteRowsEventData]()
      // FIXME: handle table being None
      val table = findTable(evData.getTableId).get
      InsertMutation(table, createRows(table, evData.getRows))

    case eventType if EventType.isUpdate(eventType) ⇒
      val evData = event.getData[UpdateRowsEventData]()
      // FIXME: handle table being None
      val table = findTable(evData.getTableId).get
      UpdateMutation(table, createRowsUpdate(table, evData.getRows))

    case eventType if EventType.isDelete(eventType) ⇒
      val evData = event.getData[DeleteRowsEventData]()
      // FIXME: handle table being None
      val table = findTable(evData.getTableId).get
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
