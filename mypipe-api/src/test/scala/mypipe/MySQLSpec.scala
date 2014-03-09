package mypipe

import mypipe.mysql.{ Listener, BinlogFilePos, BinlogConsumer }
import com.github.mauricio.async.db.{ Connection, Configuration }
import com.github.mauricio.async.db.mysql.MySQLConnection
import scala.concurrent.{ Future, Await }
import scala.concurrent.duration._
import mypipe.api._
import mypipe.producer.QueueProducer
import java.util.concurrent.{ TimeUnit, LinkedBlockingQueue }
import akka.actor.ActorSystem
import org.scalatest.BeforeAndAfterAll
import mypipe.api.UpdateMutation
import scala.Some
import mypipe.mysql.BinlogConsumer
import mypipe.api.InsertMutation

case class Db(hostname: String, port: Int, username: String, password: String, dbName: String) {

  private val configuration = new Configuration(username, hostname, port, Some(password), Some(dbName))
  var connection: Connection = _

  def connect: Unit = connect()
  def connect(timeoutMillis: Int = 5000) {
    connection = new MySQLConnection(configuration)
    val future = connection.connect
    Await.result(future, timeoutMillis seconds)
  }

  def disconnect {
    connection.disconnect
  }
}

trait DatabaseSpec {

  val name = "mypipe"
  val hostname = "blackhowler.gene"
  val port = 3306
  val username = "root"
  val password = "foobar"
  val db = Db(hostname, port, username, password, name)

  def withDatabase(testCode: Db ⇒ Any) {
    try {
      db.connect
      testCode(db)
    } finally db.disconnect
  }
}

trait ActorSystemSpec {
  val system = ActorSystem("mypipe-tests")
  implicit val ec = system.dispatcher
}

object Queries {

  object INSERT {
    val statement = """INSERT INTO user values (NULL, "username", "password")"""
    val fields = List("id", "username", "password")
  }

  object UPDATE {
    val statement = """UPDATE user set username = "username2", password = "password2""""
    val fields = List("id", "username", "password")
  }

  object TRUNCATE {
    val statment = """TRUNCATE user"""
  }

  object DELETE {
    val statement = """DELETE from user"""
  }
}

class MySQLSpec extends UnitSpec with DatabaseSpec with ActorSystemSpec with BeforeAndAfterAll {

  @volatile var connected = false

  var f: Future[Unit] = _

  val queue = new LinkedBlockingQueue[Mutation[_]]()
  val queueProducer = new QueueProducer(queue)

  val consumer = BinlogConsumer(hostname, port, username, password, BinlogFilePos.current)

  consumer.registerProducer("queue", queueProducer)
  consumer.registerListener(new Listener() {
    def onConnect(c: BinlogConsumer) {
      connected = true
    }

    def onDisconnect(c: BinlogConsumer) = {}
  })

  override def beforeAll() {
    f = Future {
      consumer.connect()
    }

    db.connect
    while (!connected) { Thread.sleep(1) }
  }

  override def afterAll() {
    db.disconnect
    consumer.disconnect()
    Await.result(f, 30 seconds)
  }

  "A binlog consumer" should "properly consume insert events" in withDatabase { db ⇒

    db.connection.sendQuery(Queries.INSERT.statement)

    Log.info("Waiting for binary log event to arrive.")
    val mutation = queue.poll(30, TimeUnit.SECONDS)

    // expect the row back
    assert(mutation != null)
    assert(mutation.isInstanceOf[InsertMutation])
  }

  "A binlog consumer" should "properly consume update events" in withDatabase { db ⇒

    db.connection.sendQuery(Queries.UPDATE.statement)

    Log.info("Waiting for binary log event to arrive.")
    val mutation = queue.poll(30, TimeUnit.SECONDS)

    // expect the row back
    assert(mutation != null)
    assert(mutation.isInstanceOf[UpdateMutation])
  }

  "A binlog consumer" should "properly consume delete events" in withDatabase { db ⇒

    db.connection.sendQuery(Queries.DELETE.statement)

    Log.info("Waiting for binary log event to arrive.")
    val mutation = queue.poll(30, TimeUnit.SECONDS)

    // expect the row back
    assert(mutation != null)
    assert(mutation.isInstanceOf[DeleteMutation])
  }
}
