package mypipe

import mypipe.mysql._
import scala.concurrent.{ Future, Await }
import scala.concurrent.duration._
import mypipe.api._
import mypipe.producer.QueueProducer
import java.util.concurrent.{ TimeUnit, LinkedBlockingQueue }
import akka.actor.ActorDSL._
import akka.pattern.ask
import org.scalatest.BeforeAndAfterAll
import mypipe.api.InsertMutation
import akka.util.Timeout
import akka.agent.Agent
import scala.collection.mutable.ListBuffer
import mypipe.api.PrimaryKey
import mypipe.api.ColumnMetadata
import scala.Some

class MySQLMetaDataSpec extends UnitSpec with DatabaseSpec with ActorSystemSpec with BeforeAndAfterAll {

  @volatile var connected = false

  override def beforeAll() {
    db.connect
  }

  override def afterAll() {
    try {
      db.disconnect
    } catch { case t: Throwable ⇒ }
  }

  implicit val timeout = Timeout(1 second)

  "MySQLMetadataManager" should "be able to fetch metadata for tables" in {

    implicit val timeout = Timeout(1 second)

    val manager = system.actorOf(
      MySQLMetadataManager.props(hostname, port.toInt, username, Some(password)),
      s"TestDBMetadataActor-$hostname:$port")

    val columnTypes: Array[ColumnType.EnumVal] = Array(ColumnType.INT24, ColumnType.VARCHAR, ColumnType.VARCHAR, ColumnType.INT24)
    val future = ask(manager, GetColumns("mypipe", "user", columnTypes)).asInstanceOf[Future[(List[ColumnMetadata], Option[PrimaryKey])]]
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
