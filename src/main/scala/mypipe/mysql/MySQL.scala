package mypipe.mysql

import com.github.shyiko.mysql.binlog.BinaryLogClient.{ LifecycleListener, EventListener }
import com.github.shyiko.mysql.binlog.BinaryLogClient
import com.github.shyiko.mysql.binlog.event.EventType._
import com.github.shyiko.mysql.binlog.event._

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import mypipe.Conf
import mypipe.api._
import com.github.mauricio.async.db.{ QueryResult, Connection, Configuration }
import com.github.mauricio.async.db.mysql.MySQLConnection
import scala.concurrent.{ Future, Await }
import akka.actor.{ Cancellable, ActorSystem }
import akka.actor.ActorDSL._
import akka.pattern.ask
import scala.collection.immutable.ListMap
import mypipe.api.PrimaryKey
import mypipe.api.UpdateMutation
import scala.Some
import mypipe.api.Column
import mypipe.api.DeleteMutation
import mypipe.api.Table
import mypipe.api.Row
import mypipe.api.InsertMutation
import akka.util.Timeout

case class BinlogFilePos(filename: String, pos: Long) {
  override def toString(): String = s"$filename:$pos"
  override def equals(o: Any): Boolean = o.asInstanceOf[BinlogFilePos].filename.equals(filename) && o.asInstanceOf[BinlogFilePos].pos == pos
}

object BinlogFilePos {
  val current = BinlogFilePos("", 0)
}

trait Listener {
  def onConnect(consumer: BinlogConsumer)
  def onDisconnect(consumer: BinlogConsumer)
}

object MySQLServerId {

  implicit val system = ActorSystem("mypipe")
  implicit val ec = system.dispatcher
  implicit val timeout = Timeout(1 second)

  case object Next

  val a = actor(new Act {
    var id = Conf.MYSQL_SERVER_ID_PREFIX
    become {
      case Next ⇒ {
        id += 1
        sender ! id
      }
    }
  })

  def next: Int = {

    val n = ask(a, Next)
    Await.result(n, 1 second).asInstanceOf[Int]
  }
}

case class BinlogConsumer(hostname: String, port: Int, username: String, password: String, binlogFileAndPos: BinlogFilePos) {

  protected val tablesById = scala.collection.mutable.HashMap[Long, Table]()
  protected var transactionInProgress = false
  protected val groupEventsByTx = Conf.GROUP_EVENTS_BY_TX
  protected val producers = new scala.collection.mutable.HashMap[String, Producer]()
  protected val listeners = new scala.collection.mutable.HashSet[Listener]()
  protected val txQueue = new scala.collection.mutable.ListBuffer[Event]
  protected val dbConns = scala.collection.mutable.HashMap[String, List[Connection]]()
  protected val dbTableCols = scala.collection.mutable.HashMap[String, (List[ColumnMetadata], Option[PrimaryKey])]()
  protected val system = ActorSystem("mypipe")
  protected implicit val ec = system.dispatcher
  protected var flusher: Option[Cancellable] = None
  protected val client = new BinaryLogClient(hostname, port, username, password)
  protected val self = this

  if (binlogFileAndPos != BinlogFilePos.current) {
    Log.info(s"Resuming binlog consumption from file=${binlogFileAndPos.filename} pos=${binlogFileAndPos.pos} for $hostname:$port")
    client.setBinlogFilename(binlogFileAndPos.filename)
    client.setBinlogPosition(binlogFileAndPos.pos)
  } else {
    Log.info(s"Using current master binlog position for consuming from $hostname:$port")
  }

  client.setServerId(MySQLServerId.next)

  client.registerEventListener(new EventListener() {
    override def onEvent(event: Event) {

      val eventType = event.getHeader().asInstanceOf[EventHeader].getEventType()

      eventType match {
        case TABLE_MAP ⇒ handleTableMap(event)
        case QUERY ⇒ handleQuery(event)
        case XID ⇒ handleXid(event)
        case e: EventType if isMutation(eventType) == true ⇒ handleMutation(event)
        case _ ⇒ Log.finer(s"Event ignored ${eventType}")
      }
    }
  })

  client.registerLifecycleListener(new LifecycleListener {
    override def onDisconnect(client: BinaryLogClient): Unit = {
      flusher.foreach(_.cancel())
      producers foreach ({
        case (n, p) ⇒ {
          Conf.binlogFilePosSave(hostname, port, BinlogFilePos(client.getBinlogFilename, client.getBinlogPosition), n)
          p.flush
        }
      })

      listeners foreach (l ⇒ l.onDisconnect(self))
    }

    override def onConnect(client: BinaryLogClient) {
      flusher = Some(system.scheduler.schedule(
        Conf.FLUSH_INTERVAL_SECS seconds,
        Conf.FLUSH_INTERVAL_SECS seconds) {
          producers foreach ({
            case (n, p) ⇒ {
              Conf.binlogFilePosSave(hostname, port, BinlogFilePos(client.getBinlogFilename, client.getBinlogPosition), n)
              p.flush
            }
          })
        })

      listeners foreach (l ⇒ l.onConnect(self))
    }

    override def onEventDeserializationFailure(client: BinaryLogClient, ex: Exception) {}
    override def onCommunicationFailure(client: BinaryLogClient, ex: Exception) {}
  })

  protected def handleMutation(event: Event) {
    if (groupEventsByTx) {
      txQueue += event
    } else {
      producers.values foreach (p ⇒ p.queue(createMutation(event)))
    }
  }

  protected def handleTableMap(event: Event) {
    val tableMapEventData: TableMapEventData = event.getData();

    if (!tablesById.contains(tableMapEventData.getTableId)) {
      val columns = getColumns(tableMapEventData.getDatabase(), tableMapEventData.getTable(), tableMapEventData.getColumnTypes())
      val table = Table(tableMapEventData.getTableId(), tableMapEventData.getTable(), tableMapEventData.getDatabase(), tableMapEventData, columns._1, columns._2)
      tablesById.put(tableMapEventData.getTableId(), table)
    }
  }

  protected def handleXid(event: Event) {
    if (groupEventsByTx) {
      commit()
    }
  }

  protected def handleQuery(event: Event) {
    if (groupEventsByTx) {
      val queryEventData: QueryEventData = event.getData()
      val query = queryEventData.getSql()
      if (groupEventsByTx) {
        if ("BEGIN".equals(query)) {
          transactionInProgress = true
        } else if ("COMMIT".equals(query)) {
          commit()
        } else if ("ROLLBACK".equals(query)) {
          rollback()
        }
      }
    }
  }

  protected def getColumns(db: String, table: String, columnTypes: Array[Byte]): (List[ColumnMetadata], Option[PrimaryKey]) = {

    val cols = dbTableCols.getOrElseUpdate(s"$db.$table", {
      val dbConn = dbConns.getOrElseUpdate(db, {
        val configuration = new Configuration(username, hostname, port, Some(password), Some("information_schema"))
        val connection1: Connection = new MySQLConnection(configuration)
        val connection2: Connection = new MySQLConnection(configuration)
        val futures = Future.sequence(List(connection1.connect, connection2.connect))
        Await.result(futures, 5 seconds)
        List(connection1, connection2)
      })

      val futureCols: Future[QueryResult] = dbConn(0).sendQuery(
        s"""select COLUMN_NAME, COLUMN_KEY from COLUMNS where TABLE_SCHEMA="$db" and TABLE_NAME = "$table" order by ORDINAL_POSITION""")

      val mapCols: Future[List[(String, Boolean)]] = futureCols.map(queryResult ⇒ queryResult.rows match {
        case Some(resultSet) ⇒ {
          resultSet.map(row ⇒ {
            (row(0).asInstanceOf[String], row(1).equals("PRI"))
          }).toList
        }

        case None ⇒ List.empty[(String, Boolean)]
      })

      val futurePkey: Future[QueryResult] = dbConn(1).sendQuery(
        s"""SELECT COLUMN_NAME FROM KEY_COLUMN_USAGE WHERE TABLE_SCHEMA='${db}' and TABLE_NAME='${table}' AND CONSTRAINT_NAME='PRIMARY' ORDER BY ORDINAL_POSITION""")

      val pKey: Future[List[String]] = futurePkey.map(queryResult ⇒ queryResult.rows match {
        case Some(resultSet) ⇒ {
          resultSet.map(row ⇒ {
            row(0).asInstanceOf[String]
          }).toList
        }

        case None ⇒ List.empty[String]
      })

      val results = Await.result(Future.sequence(List(mapCols, pKey)), 1 seconds)
      val results1 = results(0)
      val results2 = results(1)

      val cols = createColumns(results1.asInstanceOf[List[(String, Boolean)]], columnTypes)
      val primaryKey: Option[PrimaryKey] = try {
        val primaryKeys: List[ColumnMetadata] = results2.asInstanceOf[List[String]].map(colName ⇒ cols.find(_.name.equals(colName)).get)
        Some(PrimaryKey(primaryKeys))
      } catch {
        case t: Throwable ⇒ None
      }

      (cols, primaryKey)
    })

    cols
  }

  protected def createColumns(columns: List[(String, Boolean)], columnTypes: Array[Byte]): List[ColumnMetadata] = {
    try {
      // TODO: if the table definition changes we'll overflow due to the following being larger than colTypes
      var cur = 0

      val cols = columns.map(c ⇒ {
        val colName = c._1
        val isPrimaryKey = c._2
        val colType = ColumnMetadata.typeByCode(columnTypes(cur))
        cur += 1
        ColumnMetadata(colName, colType, isPrimaryKey)
      })

      cols

    } catch {
      case e: Exception ⇒ {
        Log.severe(s"Failed to determine column names: $columns\n${e.getMessage} -> ${e.getStackTraceString}")
        List.empty[ColumnMetadata]
      }
    }
  }

  protected def rollback() {
    txQueue.clear
    transactionInProgress = false
  }

  protected def commit() {
    val mutations = txQueue.map(createMutation(_))
    producers.values foreach (p ⇒ p.queueList(mutations.toList))
    txQueue.clear
    transactionInProgress = false
  }

  override def toString(): String = s"$hostname:$port"

  def connect() {
    client.connect()
  }

  def disconnect() {
    client.disconnect()
    flusher.foreach(_.cancel())
    producers.values foreach (p ⇒ p.flush)
  }

  def registerProducer(id: String, producer: Producer) {
    producers += (id -> producer)
  }

  def registerListener(listener: Listener) {
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
      val table = tablesById.get(evData.getTableId).get
      InsertMutation(table, createRows(table, evData.getRows()))
    }

    case PRE_GA_UPDATE_ROWS | UPDATE_ROWS | EXT_UPDATE_ROWS ⇒ {
      val evData = event.getData[UpdateRowsEventData]()
      val table = tablesById.get(evData.getTableId).get
      UpdateMutation(table, createRowsUpdate(table, evData.getRows()))
    }

    case PRE_GA_DELETE_ROWS | DELETE_ROWS | EXT_DELETE_ROWS ⇒ {
      val evData = event.getData[DeleteRowsEventData]()
      val table = tablesById.get(evData.getTableId).get
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

class HostPortUserPass(val host: String, val port: Int, val user: String, val password: String)
object HostPortUserPass {

  def apply(hostPortUserPass: String) = {
    val params = hostPortUserPass.split(":")
    new HostPortUserPass(params(0), params(1).toInt, params(2), params(3))
  }
}

