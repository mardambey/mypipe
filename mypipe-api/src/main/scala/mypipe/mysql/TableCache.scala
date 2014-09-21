package mypipe.mysql

import scala.concurrent.duration._
import mypipe.api._
import scala.concurrent.{ Future, Await }
import akka.actor._
import akka.pattern.ask
import akka.util.Timeout
import mypipe.api.PrimaryKey
import mypipe.api.Table

class TableCache(hostname: String, port: Int, username: String, password: String) {
  protected val system = ActorSystem("mypipe")
  protected implicit val ec = system.dispatcher
  protected val tablesById = scala.collection.mutable.HashMap[Long, Table]()
  protected val dbMetadata = system.actorOf(MySQLMetadataManager.props(hostname, port, username, Some(password)), s"DBMetadataActor-$hostname:$port")

  def getTable(tableId: Long): Option[Table] = {
    tablesById.get(tableId)
  }

  def addTableByEvent(ev: TableMapEvent): Table = {

    tablesById.getOrElseUpdate(ev.tableId, {

      // TODO: make this configurable
      implicit val timeout = Timeout(2 second)

      val future = ask(dbMetadata, GetColumns(ev.database, ev.tableName)).asInstanceOf[Future[(List[ColumnMetadata], Option[PrimaryKey])]]
      val columns = Await.result(future, 2 seconds)

      Table(ev.tableId, ev.tableName, ev.database, columns._1, columns._2)
    })
  }
}
