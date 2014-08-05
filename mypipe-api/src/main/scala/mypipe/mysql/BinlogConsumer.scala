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

case class BinlogConsumer(hostname: String, port: Int, username: String, password: String, binlogFileAndPos: BinlogFilePos) {

  protected var tableCache = new TableCache(hostname, port, username, password)
  protected var transactionInProgress = false
  protected val groupEventsByTx = Conf.GROUP_EVENTS_BY_TX
  protected val listeners = new scala.collection.mutable.HashSet[BinlogConsumerListener]()
  protected val txQueue = new scala.collection.mutable.ListBuffer[Event]
  protected val self = this
  val client = new BinaryLogClient(hostname, port, username, password)
  protected val dbMetadata = MySQLMetadataManager(hostname, port, username, Some(password))
  protected val log = LoggerFactory.getLogger(getClass)

  // FIXME: this needs to be configurable
  protected val quitOnEventHandleFailure = true

  if (binlogFileAndPos != BinlogFilePos.current) {
    log.info(s"Resuming binlog consumption from file=${binlogFileAndPos.filename} pos=${binlogFileAndPos.pos} for $hostname:$port")
    client.setBinlogFilename(binlogFileAndPos.filename)
    client.setBinlogPosition(binlogFileAndPos.pos)
  } else {
    log.info(s"Using current master binlog position for consuming from $hostname:$port")
  }

  client.setServerId(MySQLServerId.next)

  client.registerEventListener(new EventListener() {
    override def onEvent(event: Event) {

      val eventType = event.getHeader().asInstanceOf[EventHeader].getEventType()

      eventType match {
        case TABLE_MAP ⇒ tableCache.addTableByEvent(event)
        case QUERY     ⇒ handleQuery(event)
        case XID       ⇒ handleXid(event)
        case e: EventType if isMutation(eventType) == true ⇒ {
          if (!handleMutation(event) && quitOnEventHandleFailure) {
            log.error(s"Failed to process event $event and asked to quit on event handler failure, disconnecting from $hostname:$port")
            client.disconnect()
          }
        }
        case _ ⇒ log.debug(s"Event ignored ${eventType}")
      }
    }
  })

  client.registerLifecycleListener(new LifecycleListener {
    override def onDisconnect(client: BinaryLogClient): Unit = {
      listeners foreach (l ⇒ l.onDisconnect(self))
    }

    override def onConnect(client: BinaryLogClient) {
      listeners foreach (l ⇒ l.onConnect(self))
    }

    override def onEventDeserializationFailure(client: BinaryLogClient, ex: Exception) {}
    override def onCommunicationFailure(client: BinaryLogClient, ex: Exception) {}
  })

  protected def handleMutation(event: Event): Boolean = {

    if (groupEventsByTx) {

      txQueue += event
      true

    } else {

      val results = listeners.takeWhile(l ⇒ try { l.onMutation(self, createMutation(event)) }
      catch {
        case e: Exception ⇒
          log.error("Listener $l failed on mutation from event: $event")
          false
      })

      // make sure all listeners have returned true
      if (results.size == listeners.size) true
      // TODO: we know which listener failed, we can do something about it
      else false
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
    listeners.foreach(_.onMutation(this, mutations))
    txQueue.clear
    transactionInProgress = false
  }

  override def toString(): String = s"$hostname:$port"

  def connect() {
    client.connect()
  }

  def disconnect() {
    client.disconnect()
  }

  def registerListener(listener: BinlogConsumerListener) {
    listeners += listener
  }

  protected def isMutation(eventType: EventType): Boolean = eventType match {
    case PRE_GA_WRITE_ROWS | WRITE_ROWS | EXT_WRITE_ROWS |
      PRE_GA_UPDATE_ROWS | UPDATE_ROWS | EXT_UPDATE_ROWS |
      PRE_GA_DELETE_ROWS | DELETE_ROWS | EXT_DELETE_ROWS ⇒ true
    case _ ⇒ false
  }

  protected def createMutation(event: Event): Mutation[_] = event.getHeader().asInstanceOf[EventHeader].getEventType() match {
    case PRE_GA_WRITE_ROWS | WRITE_ROWS | EXT_WRITE_ROWS ⇒ {
      val evData = event.getData[WriteRowsEventData]()
      val table = tableCache.getTable(evData.getTableId()).get
      InsertMutation(table, createRows(table, evData.getRows()))
    }

    case PRE_GA_UPDATE_ROWS | UPDATE_ROWS | EXT_UPDATE_ROWS ⇒ {
      val evData = event.getData[UpdateRowsEventData]()
      val table = tableCache.getTable(evData.getTableId).get
      UpdateMutation(table, createRowsUpdate(table, evData.getRows()))
    }

    case PRE_GA_DELETE_ROWS | DELETE_ROWS | EXT_DELETE_ROWS ⇒ {
      val evData = event.getData[DeleteRowsEventData]()
      val table = tableCache.getTable(evData.getTableId).get
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

