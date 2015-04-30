package mypipe

import mypipe.api.data.{ ColumnType, ColumnMetadata, PrimaryKey }
import mypipe.mysql._
import scala.concurrent.{ Future, Await }
import scala.concurrent.duration._
import akka.pattern.ask
import akka.util.Timeout

class MySQLMetaDataSpec extends UnitSpec with DatabaseSpec with ActorSystemSpec {

  implicit val timeout = Timeout(1 second)

  "MySQLMetadataManager" should "be able to fetch metadata for tables" in {

    implicit val timeout = Timeout(1 second)

    val manager = system.actorOf(
      MySQLMetadataManager.props(Queries.DATABASE.host, Queries.DATABASE.port, Queries.DATABASE.username, Some(Queries.DATABASE.password)),
      s"TestDBMetadataActor-${Queries.DATABASE.host}:${Queries.DATABASE.port}")

    val future = ask(manager, GetColumns("mypipe", "user")).asInstanceOf[Future[(List[ColumnMetadata], Option[PrimaryKey])]]
    val c = Await.result(future, 2 seconds)
    val columns = c._1
    val pKey = c._2

    assert(columns(0).name == "id")
    assert(columns(0).colType == ColumnType.INT24)
    assert(columns(1).name == "username")
    assert(columns(1).colType == ColumnType.VARCHAR)
    assert(columns(2).name == "password")
    assert(columns(2).colType == ColumnType.VARCHAR)
    assert(columns(3).name == "login_count")
    assert(columns(3).colType == ColumnType.INT24)
    assert(pKey.isDefined == true)
    assert(pKey.get.columns.size == 1)
    assert(pKey.get.columns(0).name == "id")
  }
}
