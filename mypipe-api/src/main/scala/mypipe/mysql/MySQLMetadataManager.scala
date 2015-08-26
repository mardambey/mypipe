package mypipe.mysql

import mypipe.api.data.{ ColumnMetadata, PrimaryKey, ColumnType }

import org.slf4j.LoggerFactory
import scala.concurrent.{ Await, Future }
import scala.concurrent.duration._
import com.github.mauricio.async.db.{ Configuration, Connection, QueryResult }
import akka.actor.SupervisorStrategy.Restart
import akka.actor._
import com.github.mauricio.async.db.mysql.MySQLConnection

case class GetColumns(database: String, table: String, flushCache: Boolean = false)

object MySQLMetadataManager {
  protected val system = ActorSystem("mypipe")
  def props(hostname: String, port: Int, username: String, password: Option[String] = None, database: Option[String] = Some("information_schema")): Props = Props(new MySQLMetadataManager(hostname, port, username, password, database))
  def apply(hostname: String, port: Int, username: String, password: Option[String] = None) =
    system.actorOf(MySQLMetadataManager.props(hostname, port, username, password))
}

class MySQLMetadataManager(hostname: String, port: Int, username: String, password: Option[String] = None, database: Option[String] = Some("information_schema")) extends Actor {

  import context.dispatcher
  protected val log = LoggerFactory.getLogger(getClass)

  override val supervisorStrategy =
    OneForOneStrategy(maxNrOfRetries = 10, withinTimeRange = 1 minute) {
      case _: Exception ⇒ Restart
    }

  val configuration = new Configuration(username, hostname, port, password, database)
  protected val dbConns = scala.collection.mutable.HashMap[String, List[Connection]]()
  protected val dbTableCols = scala.collection.mutable.HashMap[String, (List[ColumnMetadata], Option[PrimaryKey])]()

  def receive = {
    case GetColumns(db, table, flushCache) ⇒ sender ! getTableColumns(db, table, flushCache)
  }

  protected def disconnectAll(): Unit = {
    for {
      (_, connections) ← dbConns
      connection ← connections
    } connection.disconnect
    dbConns.clear()
  }

  override def postStop(): Unit = {
    disconnectAll()
    super.postStop()
  }

  override def preRestart(reason: Throwable, message: Option[Any]): Unit = {
    disconnectAll()
    super.preRestart(reason, message)
  }

  protected def getTableColumns(db: String, table: String, flushCache: Boolean): (List[ColumnMetadata], Option[PrimaryKey]) = {

    if (flushCache) dbTableCols.remove(s"$db.$table")

    val cols = dbTableCols.getOrElseUpdate(s"$db.$table", {
      val dbConn = dbConns.getOrElseUpdate(db, {
        val connection1: Connection = new MySQLConnection(configuration)
        val connection2: Connection = new MySQLConnection(configuration)
        val futures = Future.sequence(List(connection1.connect, connection2.connect))
        Await.result(futures, 5 seconds)
        List(connection1, connection2)
      })

      val mapCols = getTableColumns(db, table, dbConn(0))
      val pKey = getPrimaryKey(db, table, dbConn(1))

      val results = Await.result(Future.sequence(List(mapCols, pKey)), 1 seconds)
      val results1 = results(0)
      val results2 = results(1)

      val cols = createColumns(results1.asInstanceOf[List[(String, String, Boolean)]])
      val primaryKey: Option[PrimaryKey] = try {
        val primaryKeyCols: Option[List[ColumnMetadata]] = results2.asInstanceOf[Option[List[String]]].map(colNames ⇒ {
          colNames.map(colName ⇒ {
            cols.find(_.name.equals(colName)).get
          })
        })

        primaryKeyCols.map(PrimaryKey(_))
      } catch {
        case t: Throwable ⇒ None
      }

      (cols, primaryKey)
    })

    cols
  }

  protected def getTableColumns(db: String, table: String, dbConn: Connection): Future[List[(String, String, Boolean)]] = {
    val futureCols: Future[QueryResult] = dbConn.sendQuery(
      // TODO: move this into the config file
      s"""select COLUMN_NAME, DATA_TYPE, COLUMN_KEY from COLUMNS where TABLE_SCHEMA="$db" and TABLE_NAME = "$table" order by ORDINAL_POSITION""")

    val mapCols: Future[List[(String, String, Boolean)]] = futureCols.map(queryResult ⇒ queryResult.rows match {
      case Some(resultSet) ⇒ {
        resultSet.map(row ⇒ {
          (row(0).asInstanceOf[String], row(1).asInstanceOf[String], row(2).equals("PRI"))
        }).toList
      }

      case None ⇒ {
        List.empty[(String, String, Boolean)]
      }
    })
    mapCols
  }

  protected def getPrimaryKey(db: String, table: String, dbConn: Connection): Future[Option[List[String]]] = {
    val futurePkey: Future[QueryResult] = dbConn.sendQuery(
      // TODO: move this into the config file
      s"""select COLUMN_NAME from KEY_COLUMN_USAGE where TABLE_SCHEMA='${db}' and TABLE_NAME='${table}' and CONSTRAINT_NAME='PRIMARY' order by ORDINAL_POSITION""")

    val pKey: Future[Option[List[String]]] = futurePkey.map(queryResult ⇒ queryResult.rows match {

      case Some(resultSet) if (resultSet.nonEmpty) ⇒ {
        Some(resultSet.map(row ⇒ row(0).asInstanceOf[String]).toList)
      }

      case Some(resultSet) ⇒ {
        log.debug(s"No primary key determined for $db:$table")
        None
      }

      case None ⇒ {
        log.error(s"Failed to determine primary key for $db:$table")
        None
      }
    })

    pKey
  }

  protected def createColumns(columns: List[(String, String, Boolean)]): List[ColumnMetadata] = {
    try {
      // TODO: if the table definition changes we'll overflow due to the following being larger than colTypes
      var cur = 0

      val cols = columns.map(c ⇒ {
        val colName = c._1
        val colTypeStr = c._2
        val isPrimaryKey = c._3
        val colType = ColumnType.typeByString(colTypeStr).getOrElse(ColumnType.UNKNOWN)
        cur += 1
        ColumnMetadata(colName, colType, isPrimaryKey)
      })

      cols

    } catch {
      case e: Exception ⇒ {
        log.error(s"Failed to determine column names: $columns\n${e.getMessage} -> ${e.getStackTraceString}")
        List.empty[ColumnMetadata]
      }
    }
  }
}

