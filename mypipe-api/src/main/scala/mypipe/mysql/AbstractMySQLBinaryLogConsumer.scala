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
import scala.concurrent.blocking
import scala.concurrent.{ Await, Future }
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration

import java.util.concurrent.ThreadLocalRandom

abstract class AbstractMySQLBinaryLogConsumer
    extends AbstractBinaryLogConsumer[MEvent]
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
    val header = event.getHeader[EventHeader]
    val timestamp = header.getTimestamp
    header.getEventType match {
      case TABLE_MAP                     ⇒ decodeTableMapEvent(timestamp, event)
      case QUERY                         ⇒ decodeQueryEvent(timestamp, event)
      case XID                           ⇒ decodeXidEvent(timestamp, event)
      case e: EventType if isMutation(e) ⇒ decodeMutationEvent(timestamp, event)
      case _                             ⇒ Some(UnknownEvent(timestamp))
    }
  }

  private def decodeTableMapEvent(timestamp: Long, event: MEvent): Option[TableMapEvent] = {
    val tableMapEventData = event.getData[TableMapEventData]
    Some(TableMapEvent(
      timestamp,
      tableMapEventData.getTableId,
      tableMapEventData.getTable,
      tableMapEventData.getDatabase,
      tableMapEventData.getColumnTypes))
  }

  protected def decodeQueryEvent(timestamp: Long, event: MEvent): Option[QueryEvent] = {
    val queryEventData = event.getData[QueryEventData]
    val query = queryEventData.getSql

    Some(query.toLowerCase match {
      case q if q.indexOf("begin") == 0 ⇒
        BeginEvent(timestamp, queryEventData.getDatabase, queryEventData.getSql)
      case q if q.indexOf("commit") == 0 ⇒
        CommitEvent(timestamp, queryEventData.getDatabase, queryEventData.getSql)
      case q if q.indexOf("rollback") == 0 ⇒
        RollbackEvent(timestamp, queryEventData.getDatabase, queryEventData.getSql)
      case q if q.indexOf("alter") == 0 ⇒
        val databaseName = {
          if (queryEventData.getDatabase.nonEmpty) queryEventData.getDatabase
          else decodeDatabaseFromAlter(queryEventData.getSql)
        }

        val tableName = decodeTableFromAlter(queryEventData.getSql)
        val table = findTable(databaseName, tableName).getOrElse(new UnknownTable(-1.toLong, name = tableName, db = databaseName))
        AlterEvent(timestamp, table, queryEventData.getSql)
      case q ⇒
        log.debug("Consumer {} ignoring unknown query query={}", Array(id, q): _*)
        UnknownQueryEvent(timestamp, queryEventData.getDatabase, queryEventData.getSql)
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

  protected def decodeXidEvent(timestamp: Long, event: MEvent): Option[XidEvent] = {
    val eventData = event.getData[XidEventData]
    Some(XidEvent(timestamp, eventData.getXid))
  }

  protected def decodeMutationEvent(timestamp: Long, event: MEvent): Option[Mutation] = {
    Some(createMutation(timestamp, event))
  }

  override def toString: String = s"$hostname:$port"

  /** Creates random mysql server id. */
  private def nextServerId(): Long = {
    // MySQL server id should be between 1 (inclusive) and 4294967295 (inclusive)
    // Source: https://dev.mysql.com/doc/refman/5.7/en/replication-options.html

    val origin = 1L
    val bound = 1L << 32

    // Format is nextLong(lower_inclusive, upper_exclusive), or lower <= x < upper
    ThreadLocalRandom.current().nextLong(origin, bound)
  }

  override protected def onStart(): Future[Boolean] = {
    @volatile var connected = false
    client.setServerId(nextServerId())
    client.registerEventListener(new EventListener() {
      override def onEvent(event: MEvent) = handleEvent(event)
    })

    client.registerLifecycleListener(new LifecycleListener {
      override def onDisconnect(client: BinaryLogClient) = handleDisconnect()
      override def onConnect(client: BinaryLogClient) = {
        log.info(s"connected = true")
        connected = true
        handleConnect()
      }
      override def onEventDeserializationFailure(client: BinaryLogClient, ex: Exception) {}
      override def onCommunicationFailure(client: BinaryLogClient, ex: Exception) {}
    })

    log.info(s"Connecting client to $client.get:$hostname:$port")

    // `client.connect()` blocks, so use a Thread instead of a Future
    val clientThread = new Thread(new Runnable {
      def run() {
        client.connect()
      }
    })
    clientThread.start

    val startTime = Platform.currentTime
    while (Platform.currentTime - startTime < 10000 && !connected) Thread.sleep(10)

    Future.successful(connected)
  }

  override protected def onStop(): Unit = client.disconnect()

  protected def isMutation(eventType: EventType): Boolean = {
    EventType.isDelete(eventType) ||
      EventType.isUpdate(eventType) ||
      EventType.isWrite(eventType)
  }

  protected def createMutation(timestamp: Long, event: MEvent): Mutation =
    event.getHeader[EventHeader].getEventType match {
      case eventType if EventType.isWrite(eventType) ⇒
        val evData = event.getData[WriteRowsEventData]()
        // FIXME: handle table being None
        val table = findTable(evData.getTableId).get
        InsertMutation(timestamp, table, createRows(table, evData.getRows))

      case eventType if EventType.isUpdate(eventType) ⇒
        val evData = event.getData[UpdateRowsEventData]()
        // FIXME: handle table being None
        val table = findTable(evData.getTableId).get
        UpdateMutation(timestamp, table, createRowsUpdate(table, evData.getRows))

      case eventType if EventType.isDelete(eventType) ⇒
        val evData = event.getData[DeleteRowsEventData]()
        // FIXME: handle table being None
        val table = findTable(evData.getTableId).get
        DeleteMutation(timestamp, table, createRows(table, evData.getRows))
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
