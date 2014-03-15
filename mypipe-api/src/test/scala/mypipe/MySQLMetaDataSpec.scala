package mypipe

import mypipe.mysql.{ MySQLMetadataManager, Listener, BinlogFilePos, BinlogConsumer }
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

    val manager = system.actorOf(
      MySQLMetadataManager.props(hostname, port.toInt, username, Some(password)),
      s"TestDBMetadataActor-$hostname:$port")

  }
}
