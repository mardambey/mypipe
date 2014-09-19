package mypipe.mysql

import com.github.shyiko.mysql.binlog.BinaryLogClient.{ LifecycleListener, EventListener }
import org.slf4j.LoggerFactory

import com.github.shyiko.mysql.binlog.BinaryLogClient
import com.github.shyiko.mysql.binlog.event.EventType._
import com.github.shyiko.mysql.binlog.event._

import scala.collection.JavaConverters._
import mypipe.Conf
import mypipe.api._
import scala.collection.immutable.ListMap
import mypipe.api.UpdateMutation
import mypipe.api.Column
import mypipe.api.DeleteMutation
import mypipe.api.Table
import mypipe.api.Row
import mypipe.api.InsertMutation

abstract class BaseBinaryLogConsumer(
  override val hostname: String,
  override val port: Int,
  username: String,
  password: String,
  binlogFileAndPos: BinaryLogFilePosition)
    extends BinaryLogRawConsumerTrait
    with BinaryLogConsumerTrait {

  // FIXME: this is public because tests access it directly
  val client = new BinaryLogClient(hostname, port, username, password)

  // FIXME: this needs to be configurable
  protected val quitOnEventHandleFailure = true

  protected var transactionInProgress = false
  protected val groupEventsByTx = Conf.GROUP_EVENTS_BY_TX
  protected val txQueue = new scala.collection.mutable.ListBuffer[Event]
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
    val eventType = event.getHeader().asInstanceOf[EventHeader].getEventType()

    eventType match {
      case TABLE_MAP ⇒ handleTableMap(event)
      case QUERY     ⇒ handleQuery(event)
      case XID       ⇒ handleXid(event)

      case e: EventType if isMutation(eventType) == true ⇒ {
        if (!handleMutation(event)) {

          log.error("Failed to process event $event.")

          if (quitOnEventHandleFailure) {
            log.error(s"Failure encountered and asked to quit on event handler failure, disconnecting from $hostname:$port")
            client.disconnect()
          }
        }
      }
      case _ ⇒ log.debug(s"Event ignored ${eventType}")
    }
  }

  protected def handleTableMap(event: Event) {
    val tableMapEventData: TableMapEventData = event.getData()
    val tableMapEvent = TableMapEvent(
      tableMapEventData.getTableId,
      tableMapEventData.getTable,
      tableMapEventData.getDatabase,
      tableMapEventData.getColumnTypes)
    val table = handleTableMap(tableMapEvent)
  }

  protected def handleMutation(event: Event): Boolean = {

    if (groupEventsByTx) {

      txQueue += event
      true

    } else {

      val mutation = createMutation(event)
      handleMutation(mutation)
    }
  }

  protected def handleXid(event: Event) {
    if (groupEventsByTx) {
      commit()
    }
  }

  protected def handleQuery(event: Event) {
    val queryEventData: QueryEventData = event.getData()
    val query = queryEventData.getSql()

    query match {
      case "BEGIN" if groupEventsByTx    ⇒ transactionInProgress = true
      case "COMMIT" if groupEventsByTx   ⇒ commit()
      case "ROLLBACK" if groupEventsByTx ⇒ rollback()
      case q if q.indexOf("ALTER") == 0  ⇒ handleAlter(event)
      case _                             ⇒
    }
  }

  protected def handleAlter(event: Event) {

  }

  protected def rollback() {
    txQueue.clear
    transactionInProgress = false
  }

  protected def commit() {
    val mutations = txQueue.map(createMutation(_))
    mutations.foreach(handleMutation)
    txQueue.clear
    transactionInProgress = false
  }

  override def toString(): String = s"$hostname:$port"

  def connect(): Unit = client.connect()

  def disconnect(): Unit = client.disconnect()

  protected def isMutation(eventType: EventType): Boolean = eventType match {
    case PRE_GA_WRITE_ROWS | WRITE_ROWS | EXT_WRITE_ROWS |
      PRE_GA_UPDATE_ROWS | UPDATE_ROWS | EXT_UPDATE_ROWS |
      PRE_GA_DELETE_ROWS | DELETE_ROWS | EXT_DELETE_ROWS ⇒ true
    case _ ⇒ false
  }

  protected def createMutation(event: Event): Mutation[_] = event.getHeader().asInstanceOf[EventHeader].getEventType() match {
    case PRE_GA_WRITE_ROWS | WRITE_ROWS | EXT_WRITE_ROWS ⇒ {
      val evData = event.getData[WriteRowsEventData]()
      val table = getTableById(evData.getTableId)
      InsertMutation(table, createRows(table, evData.getRows()))
    }

    case PRE_GA_UPDATE_ROWS | UPDATE_ROWS | EXT_UPDATE_ROWS ⇒ {
      val evData = event.getData[UpdateRowsEventData]()
      val table = getTableById(evData.getTableId)
      UpdateMutation(table, createRowsUpdate(table, evData.getRows()))
    }

    case PRE_GA_DELETE_ROWS | DELETE_ROWS | EXT_DELETE_ROWS ⇒ {
      val evData = event.getData[DeleteRowsEventData]()
      val table = getTableById(evData.getTableId)
      DeleteMutation(table, createRows(table, evData.getRows()))
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
