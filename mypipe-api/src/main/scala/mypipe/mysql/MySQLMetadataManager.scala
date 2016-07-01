package mypipe.mysql

import mypipe.api.data.{ColumnMetadata, PrimaryKey, ColumnType}

import org.slf4j.LoggerFactory
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import com.github.mauricio.async.db.{Configuration, Connection}
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
    OneForOneStrategy(maxNrOfRetries = 10, withinTimeRange = 1.minute) {
      case _: Exception ⇒ Restart
    }

  val configuration = new Configuration(username, hostname, port, password, database)
  protected val dbConns = scala.collection.mutable.HashMap[String, List[Connection]]()
  protected val dbTableCols = scala.collection.mutable.HashMap[String, (List[ColumnMetadata], Option[PrimaryKey])]()

  def receive = {
    case GetColumns(db, table, flushCache) ⇒
      sender ! getTableColumns(db, table, flushCache)
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
        Await.result(futures, 5.seconds)
        List(connection1, connection2)
      })

      val mapColsF = Util.getTableColumns(db, table, dbConn.head)
      val pKeyF = Util.getPrimaryKey(db, table, dbConn(1))

      val results = Await.result(Future.sequence(List(mapColsF, pKeyF)), 10.seconds)
      val mapCols = results(0)
      val pKey = results(1)

      val cols = Util.createColumns(mapCols.asInstanceOf[List[(String, String, Boolean)]])

      val primaryKey: Option[PrimaryKey] = pKey.asInstanceOf[Option[List[String]]].map(pkeyColNames ⇒ Util.getPrimaryKeyFromColumns(pkeyColNames, cols))

      (cols, primaryKey)
    })

    cols
  }
}

