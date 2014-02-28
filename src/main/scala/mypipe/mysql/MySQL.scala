package mypipe.mysql

import com.github.shyiko.mysql.binlog.BinaryLogClient.{ LifecycleListener, EventListener }
import com.github.shyiko.mysql.binlog.BinaryLogClient
import com.github.shyiko.mysql.binlog.event.EventType._
import com.github.shyiko.mysql.binlog.event._

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import mypipe.Conf
import mypipe.Log
import mypipe.api._
import mypipe.api.UpdateMutation
import mypipe.api.DeleteMutation
import mypipe.api.InsertMutation
import com.github.mauricio.async.db.{ RowData, QueryResult, Connection, Configuration }
import com.github.mauricio.async.db.mysql.MySQLConnection
import scala.concurrent.{ Future, Await }
import akka.actor.{ Cancellable, ActorSystem }

case class BinlogFilePos(filename: String, pos: Long) {
  override def toString(): String = s"$filename:$pos"
}

object BinlogFilePos {
  val current = BinlogFilePos("", 0)
}

case class BinlogConsumer(hostname: String, port: Int, username: String, password: String, binlogFileAndPos: BinlogFilePos) {

  val tablesById = scala.collection.mutable.HashMap[Long, Table]()
  var transactionInProgress = false
  val groupEventsByTx = Conf.GROUP_EVENTS_BY_TX
  val producers = new scala.collection.mutable.HashSet[Producer]()
  val txQueue = new scala.collection.mutable.ListBuffer[Event]
  val client = new BinaryLogClient(hostname, port, username, password)

  val system = ActorSystem("mypipe")
  implicit val ec = system.dispatcher

  var flusher: Option[Cancellable] = None

  client.registerEventListener(new EventListener() {

    override def onEvent(event: Event) {

      val eventType = event.getHeader().asInstanceOf[EventHeader].getEventType()
      println(event)

      eventType match {
        case TABLE_MAP ⇒ {
          val tableMapEventData: TableMapEventData = event.getData();

          if (!tablesById.contains(tableMapEventData.getTableId)) {
            val columns = getColumns(tableMapEventData.getDatabase(), tableMapEventData.getTable(), tableMapEventData.getColumnTypes())
            val table = Table(tableMapEventData.getTableId(), tableMapEventData.getTable(), tableMapEventData.getDatabase(), tableMapEventData, columns)
            tablesById.put(tableMapEventData.getTableId(), table)
          }
        }

        case e: EventType if isMutation(eventType) == true ⇒ {
          if (groupEventsByTx) {
            txQueue += event
          } else {
            producers foreach (p ⇒ p.queue(createMutation(event)))
          }
        }

        case QUERY ⇒ {
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
        case XID ⇒ {
          if (groupEventsByTx) {
            commit()
          }
        }
        case _ ⇒ Log.finer(s"Event ignored ${eventType}")
      }
    }

    val dbConns = scala.collection.mutable.HashMap[String, Connection]()
    val dbTableCols = scala.collection.mutable.HashMap[String, List[ColumnMetadata]]()
    implicit val ec = ActorSystem("mypipe").dispatcher

    def getColumns(db: String, table: String, columnTypes: Array[Byte]): List[ColumnMetadata] = {

      val cols = dbTableCols.getOrElseUpdate(s"$db.$table", {
        val dbConn = dbConns.getOrElseUpdate(db, {
          val configuration = new Configuration(username, hostname, port, Some(password), Some("information_schema"))
          val connection: Connection = new MySQLConnection(configuration)
          Await.result(connection.connect, 5 seconds)
          connection
        })

        val future: Future[QueryResult] = dbConn.sendQuery(
          s"""select COLUMN_NAME, COLUMN_KEY from COLUMNS where TABLE_SCHEMA="$db" and TABLE_NAME = "$table" order by ORDINAL_POSITION""")

        val mapResult: Future[List[(String, Boolean)]] = future.map(queryResult ⇒ queryResult.rows match {
          case Some(resultSet) ⇒ {
            resultSet.map(row ⇒ {
              (row(0).asInstanceOf[String], row(1).equals("PRI"))
            }).toList
          }

          case None ⇒ List.empty[(String, Boolean)]
        })

        val result = Await.result(mapResult, 5 seconds)
        createColumns(result, columnTypes)
      })

      cols
    }

    def createColumns(columns: List[(String, Boolean)], columnTypes: Array[Byte]): List[ColumnMetadata] = {
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

    def rollback() {
      txQueue.clear
      transactionInProgress = false
    }

    def commit() {
      val mutations = txQueue.map(createMutation(_))
      producers foreach (p ⇒ p.queueList(mutations.toList))
      txQueue.clear
      transactionInProgress = false
    }
  })

  if (binlogFileAndPos != BinlogFilePos.current) {
    Log.info(s"Resuming binlog consumption from file=${binlogFileAndPos.filename} pos=${binlogFileAndPos.pos} for $hostname:$port")
    client.setBinlogFilename(binlogFileAndPos.filename)
    client.setBinlogPosition(binlogFileAndPos.pos)
  } else {
    Log.info(s"Using current master binlog position for consuming from $hostname:$port")
  }

  client.registerLifecycleListener(new LifecycleListener {
    override def onDisconnect(client: BinaryLogClient): Unit = {
      Conf.binlogFilePosSave(hostname, port, BinlogFilePos(client.getBinlogFilename, client.getBinlogPosition))
    }

    override def onEventDeserializationFailure(client: BinaryLogClient, ex: Exception) {}
    override def onConnect(client: BinaryLogClient) {}
    override def onCommunicationFailure(client: BinaryLogClient, ex: Exception) {}
  })

  def connect() {
    flusher = Some(system.scheduler.schedule(
      Conf.FLUSH_INTERVAL_SECS seconds,
      Conf.FLUSH_INTERVAL_SECS seconds) {
        producers foreach (p ⇒ p.flush)
        Conf.binlogFilePosSave(hostname, port, BinlogFilePos(client.getBinlogFilename, client.getBinlogPosition))
      })
    client.connect()
  }

  def disconnect() {
    client.disconnect()
    flusher.foreach(_.cancel())
    producers foreach (p ⇒ p.flush)
  }

  def registerProducer(producer: Producer) {
    producers += producer
  }

  def isMutation(eventType: EventType): Boolean = eventType match {
    case PRE_GA_WRITE_ROWS | WRITE_ROWS | EXT_WRITE_ROWS |
      PRE_GA_UPDATE_ROWS | UPDATE_ROWS | EXT_UPDATE_ROWS |
      PRE_GA_DELETE_ROWS | DELETE_ROWS | EXT_DELETE_ROWS ⇒ true
    case _ ⇒ false
  }

  def createMutation(event: Event): Mutation[_] = event.getHeader().asInstanceOf[EventHeader].getEventType() match {
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
      val columns = table.columns.zip(evRow).map(c ⇒ c._1.name -> Column(c._1, c._2)).toMap[String, Column]

      Row(table, columns)

    }).toList
  }

  protected def createRowsUpdate(table: Table, evRows: java.util.List[java.util.Map.Entry[Array[java.io.Serializable], Array[java.io.Serializable]]]): List[(Row, Row)] = {
    evRows.asScala.map(evRow ⇒ {

      // zip the names and values from the table's columns and the row's data and
      // create a map that contains column names to Column objects with values

      val old = table.columns.zip(evRow.getKey).map(c ⇒ c._1.name -> Column(c._1, c._2)).toMap[String, Column]
      val cur = table.columns.zip(evRow.getValue).map(c ⇒ c._1.name -> Column(c._1, c._2)).toMap[String, Column]

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

