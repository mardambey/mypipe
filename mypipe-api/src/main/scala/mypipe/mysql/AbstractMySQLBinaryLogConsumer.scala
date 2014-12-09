package mypipe.mysql

import com.github.shyiko.mysql.binlog.BinaryLogClient.{ LifecycleListener, EventListener }
import mypipe.api.consumer.AbstractBinaryLogConsumer
import mypipe.api.data.{ Column, Table, Row }
import mypipe.api.event._
import org.slf4j.LoggerFactory

import com.github.shyiko.mysql.binlog.BinaryLogClient
import com.github.shyiko.mysql.binlog.event.EventType._
import com.github.shyiko.mysql.binlog.event._

import scala.collection.JavaConverters._
import scala.collection.immutable.ListMap

abstract class AbstractMySQLBinaryLogConsumer(
  override val hostname: String,
  override val port: Int,
  username: String,
  password: String,
  binlogFileAndPos: BinaryLogFilePosition)
    extends AbstractBinaryLogConsumer {

  // FIXME: this is public because tests access it directly
  val client = new BinaryLogClient(hostname, port, username, password)

  // FIXME: this needs to be configurable
  protected val quitOnEventHandleFailure = true
  protected val log = LoggerFactory.getLogger(getClass)

  if (binlogFileAndPos != BinaryLogFilePosition.current) {
    log.info(s"Resuming binlog consumption from file=${binlogFileAndPos.filename} pos=${binlogFileAndPos.pos} for $hostname:$port")
    client.setBinlogFilename(binlogFileAndPos.filename)
    client.setBinlogPosition(binlogFileAndPos.pos)
  } else {
    log.info(s"Using current master binlog position for consuming from $hostname:$port")
  }

  client.setServerId(MySQLServerId.next)

  client.registerEventListener(new EventListener() {
    override def onEvent(event: Event) {
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

  protected def handleError(event: Event): Unit = {

  }

  protected def handleEvent(event: Event): Unit = {
    val eventType = event.getHeader[EventHeader].getEventType

    eventType match {
      case TABLE_MAP ⇒ handleTableMap(event)
      case QUERY     ⇒ handleQuery(event)
      case XID       ⇒ handleXid(event)

      case e: EventType if isMutation(eventType) ⇒ {
        if (!handleMutation(event)) {

          log.error("Failed to process event $event.")

          if (quitOnEventHandleFailure) {
            log.error("Failure encountered and asked to quit on event handler failure, disconnecting from {}:{}", hostname, port)
            client.disconnect()
          }
        }
      }
      case _ ⇒ log.trace("Event ignored {}", eventType)
    }
  }

  protected def handleTableMap(event: Event) {
    val tableMapEventData = event.getData[TableMapEventData]
    val tableMapEvent = TableMapEvent(
      tableMapEventData.getTableId,
      tableMapEventData.getTable,
      tableMapEventData.getDatabase,
      tableMapEventData.getColumnTypes)

    handleTableMap(tableMapEvent)
  }

  protected def handleMutation(event: Event): Boolean = {
    val mutation = createMutation(event)
    handleMutation(mutation)
  }

  protected def handleXid(event: Event) {
    val eventData = event.getData[XidEventData]
    handleXid(XidEvent(eventData.getXid))
  }

  protected def handleQuery(event: Event) {
    val queryEventData = event.getData[QueryEventData]
    val query = queryEventData.getSql

    log.trace("query={}", query)

    query.toLowerCase match {
      case q if q.indexOf("begin") == 0 && groupEventsByTx ⇒
        handleBegin(BeginEvent(queryEventData.getDatabase, queryEventData.getSql))
      case q if q.indexOf("commit") == 0 && groupEventsByTx ⇒
        handleCommit(CommitEvent(queryEventData.getDatabase, queryEventData.getSql))
      case q if q.indexOf("rollback") == 0 && groupEventsByTx ⇒
        handleRollback(RollbackEvent(queryEventData.getDatabase, queryEventData.getSql))
      case q if q.indexOf("alter") == 0 ⇒
        handleAlter(AlterEvent(queryEventData.getDatabase, queryEventData.getSql))
      case q ⇒ log.trace("ignoring query={}", q)
    }
  }

  override def toString: String = s"$hostname:$port"

  def connect(): Unit = client.connect()

  def disconnect(): Unit = client.disconnect()

  protected def isMutation(eventType: EventType): Boolean = eventType match {
    case PRE_GA_WRITE_ROWS | WRITE_ROWS | EXT_WRITE_ROWS |
      PRE_GA_UPDATE_ROWS | UPDATE_ROWS | EXT_UPDATE_ROWS |
      PRE_GA_DELETE_ROWS | DELETE_ROWS | EXT_DELETE_ROWS ⇒ true
    case _ ⇒ false
  }

  protected def createMutation(event: Event): Mutation[_] = event.getHeader[EventHeader].getEventType match {
    case PRE_GA_WRITE_ROWS | WRITE_ROWS | EXT_WRITE_ROWS ⇒ {
      val evData = event.getData[WriteRowsEventData]()
      val table = getTableById(evData.getTableId)
      InsertMutation(table, createRows(table, evData.getRows))
    }

    case PRE_GA_UPDATE_ROWS | UPDATE_ROWS | EXT_UPDATE_ROWS ⇒ {
      val evData = event.getData[UpdateRowsEventData]()
      val table = getTableById(evData.getTableId)
      UpdateMutation(table, createRowsUpdate(table, evData.getRows))
    }

    case PRE_GA_DELETE_ROWS | DELETE_ROWS | EXT_DELETE_ROWS ⇒ {
      val evData = event.getData[DeleteRowsEventData]()
      val table = getTableById(evData.getTableId)
      DeleteMutation(table, createRows(table, evData.getRows))
    }
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
