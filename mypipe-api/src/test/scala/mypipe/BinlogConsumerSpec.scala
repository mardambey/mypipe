package mypipe

import mypipe.mysql.{ BinlogFilePos, BinlogConsumer }
import com.github.mauricio.async.db.{ Connection, Configuration }
import com.github.mauricio.async.db.mysql.MySQLConnection
import scala.concurrent.Await
import scala.concurrent.duration._
import mypipe.api._
import mypipe.producer.QueueProducer
import java.util.concurrent.{ TimeUnit, LinkedBlockingQueue }
import akka.actor.ActorSystem
import org.scalatest.BeforeAndAfterAll
import mypipe.api.UpdateMutation
import scala.Some
import mypipe.api.InsertMutation
import com.typesafe.config.ConfigFactory
import org.slf4j.LoggerFactory

case class Db(hostname: String, port: Int, username: String, password: String, dbName: String) {

  private val configuration = new Configuration(username, hostname, port, Some(password), Some(dbName))
  var connection: Connection = _

  def connect: Unit = connect()
  def connect(timeoutMillis: Int = 5000) {
    connection = new MySQLConnection(configuration)
    val future = connection.connect
    Await.result(future, timeoutMillis millis)
  }

  def disconnect: Unit = disconnect()
  def disconnect(timeoutMillis: Int = 5000) {
    val future = connection.disconnect
    Await.result(future, timeoutMillis millis)
  }
}

trait ConfigSpec {
  val conf = ConfigFactory.load("test.conf")
}

trait DatabaseSpec extends ConfigSpec {

  val db = Db(
    Queries.DATABASE.host,
    Queries.DATABASE.port,
    Queries.DATABASE.username,
    Queries.DATABASE.password,
    Queries.DATABASE.name)

  def withDatabase(testCode: Db ⇒ Any) {
    try {
      db.connect
      testCode(db)
    } finally db.disconnect
  }
}

trait ActorSystemSpec {
  implicit val system = ActorSystem("mypipe-tests")
  implicit val ec = system.dispatcher
}

object Queries {

  val conf = ConfigFactory.load("test.conf")
  val Array(dbHost, dbPort, dbUsername, dbPassword, dbName) = conf.getString("mypipe.test.database.info").split(":")

  object DATABASE {
    val name = dbName
    val host = dbHost
    val port = dbPort.toInt
    val username = dbUsername
    val password = dbPassword
  }

  object TABLE {
    val name = "user"
    val fields = List("id", "username", "password", "login_count")
  }

  object INSERT {
    val username = "username"
    val password = "password"
    val loginCount = 0;

    def statement: String = statement()
    def statement(id: String = "NULL", username: String = this.username, password: String = this.password, loginCount: Int = this.loginCount): String =
      s"""INSERT INTO user values ($id, "$username", "$password", $loginCount)"""
  }

  object UPDATE {
    val username = "username2"
    val password = "password2"
    lazy val statement = s"""UPDATE user set username = "$username", password = "$password", login_count = login_count + 1"""
  }

  object TRUNCATE {
    val statement = """TRUNCATE user"""
  }

  object DELETE {
    val statement = """DELETE from user"""
  }

  object CREATE {
    val statement = conf.getString("mypipe.test.database.create")
  }
}

class MySQLSpec extends UnitSpec with DatabaseSpec with ActorSystemSpec with BeforeAndAfterAll {

  val log = LoggerFactory.getLogger(getClass)
  @volatile var connected = false

  val queue = new LinkedBlockingQueue[Mutation[_]]()
  val queueProducer = new QueueProducer(queue)
  val consumer = BinlogConsumer(Queries.DATABASE.host, Queries.DATABASE.port, Queries.DATABASE.username, Queries.DATABASE.password, BinlogFilePos.current)
  val pipe = new Pipe("test-pipe", List(consumer), queueProducer)

  override def beforeAll() {

    db.connect
    pipe.connect()

    while (!db.connection.isConnected || !pipe.isConnected) { Thread.sleep(1) }

    Await.result(db.connection.sendQuery(Queries.CREATE.statement), 1 second)
    Await.result(db.connection.sendQuery(Queries.TRUNCATE.statement), 1 second)
  }

  override def afterAll() {
    pipe.disconnect()
    db.disconnect
  }

  "A binlog consumer" should "properly consume insert events" in withDatabase { db ⇒

    db.connection.sendQuery(Queries.INSERT.statement)

    log.info("Waiting for binary log event to arrive.")
    val mutation = queue.poll(30, TimeUnit.SECONDS)

    // expect the row back
    assert(mutation != null)
    assert(mutation.isInstanceOf[InsertMutation])
  }

  "A binlog consumer" should "properly consume update events" in withDatabase { db ⇒

    db.connection.sendQuery(Queries.UPDATE.statement)

    log.info("Waiting for binary log event to arrive.")
    val mutation = queue.poll(30, TimeUnit.SECONDS)

    // expect the row back
    assert(mutation != null)
    assert(mutation.isInstanceOf[UpdateMutation])
  }

  "A binlog consumer" should "properly consume delete events" in withDatabase { db ⇒

    db.connection.sendQuery(Queries.DELETE.statement)

    log.info("Waiting for binary log event to arrive.")
    val mutation = queue.poll(30, TimeUnit.SECONDS)

    // expect the row back
    assert(mutation != null)
    assert(mutation.isInstanceOf[DeleteMutation])
  }
}
