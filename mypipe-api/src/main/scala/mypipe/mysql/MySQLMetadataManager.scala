package mypipe.mysql

import mypipe.api.{ ColumnType, PrimaryKey, ColumnMetadata }

import org.slf4j.LoggerFactory
import scala.concurrent.{ Await, Future }
import scala.concurrent.duration._
import com.github.mauricio.async.db.{ Configuration, Connection, QueryResult }
import akka.actor.SupervisorStrategy.Restart
import akka.actor._
import com.github.mauricio.async.db.mysql.MySQLConnection

case class GetColumns(database: String, table: String, columnTypes: Array[ColumnType.EnumVal], flushCache: Boolean = false)

object MySQLMetadataManager {
  protected val system = ActorSystem("mypipe")
  def props(hostname: String, port: Int, username: String, password: Option[String] = None, database: Option[String] = Some("information_schema")): Props = Props(new MySQLMetadataManager(hostname, port, username, password, database))
  def apply(hostname: String, port: Int, username: String, password: Option[String] = None, instanceName: Option[String] = None) =
    system.actorOf(MySQLMetadataManager.props(hostname, port, username, password), instanceName.getOrElse(s"DBMetadataActor-$hostname:$port"))
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
    case GetColumns(db, table, colTypes, flushCache) ⇒ sender ! getTableColumns(db, table, colTypes, flushCache)
  }

  protected def getTableColumns(db: String, table: String, columnTypes: Array[ColumnType.EnumVal], flushCache: Boolean): (List[ColumnMetadata], Option[PrimaryKey]) = {

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

  protected def getTableColumns(db: String, table: String, dbConn: Connection): Future[List[(String, Boolean)]] = {
    val futureCols: Future[QueryResult] = dbConn.sendQuery(
      s"""select COLUMN_NAME, COLUMN_KEY from COLUMNS where TABLE_SCHEMA="$db" and TABLE_NAME = "$table" order by ORDINAL_POSITION""")

    val mapCols: Future[List[(String, Boolean)]] = futureCols.map(queryResult ⇒ queryResult.rows match {
      case Some(resultSet) ⇒ {
        resultSet.map(row ⇒ {
          (row(0).asInstanceOf[String], row(1).equals("PRI"))
        }).toList
      }

      case None ⇒ List.empty[(String, Boolean)]
    })
    mapCols
  }

  protected def getPrimaryKey(db: String, table: String, dbConn: Connection): Future[List[String]] = {
    val futurePkey: Future[QueryResult] = dbConn.sendQuery(
      s"""SELECT COLUMN_NAME FROM KEY_COLUMN_USAGE WHERE TABLE_SCHEMA='${db}' and TABLE_NAME='${table}' AND CONSTRAINT_NAME='PRIMARY' ORDER BY ORDINAL_POSITION""")

    val pKey: Future[List[String]] = futurePkey.map(queryResult ⇒ queryResult.rows match {
      case Some(resultSet) ⇒ {
        resultSet.map(row ⇒ {
          row(0).asInstanceOf[String]
        }).toList
      }

      case None ⇒ List.empty[String]
    })

    pKey
  }

  protected def createColumns(columns: List[(String, Boolean)], columnTypes: Array[ColumnType.EnumVal]): List[ColumnMetadata] = {
    try {
      // TODO: if the table definition changes we'll overflow due to the following being larger than colTypes
      var cur = 0

      val cols = columns.map(c ⇒ {
        val colName = c._1
        val isPrimaryKey = c._2
        val colType = columnTypes(cur)
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

