import akka.actor.ActorSystem
import com.github.mauricio.async.db.{ Configuration, Connection }
import com.github.mauricio.async.db.mysql.MySQLConnection
import com.typesafe.config.ConfigFactory
import org.scalatest.BeforeAndAfterAll
import scala.concurrent.duration._
import scala.concurrent.Await

package object mypipe {

  case class Db(hostname: String, port: Int, username: String, password: String, dbName: String) {

    private val configuration = new Configuration(username, hostname, port, Some(password))
    var connection: Connection = _

    def connect(): Unit = connect(timeoutMillis = 5000)
    def connect(timeoutMillis: Int) {
      connection = new MySQLConnection(configuration)
      val future = connection.connect
      Await.result(future, timeoutMillis.millis)
    }

    def select(db: String): Unit = {
    }

    def disconnect(): Unit = disconnect(timeoutMillis = 5000)
    def disconnect(timeoutMillis: Int) {
      val future = connection.disconnect
      Await.result(future, timeoutMillis.millis)
    }
  }

  trait ConfigSpec {
    val conf = ConfigFactory.load("test.conf")
  }

  trait DatabaseSpec extends UnitSpec with ConfigSpec with BeforeAndAfterAll {

    val db = Db(
      Queries.DATABASE.host,
      Queries.DATABASE.port,
      Queries.DATABASE.username,
      Queries.DATABASE.password,
      Queries.DATABASE.name)

    override def beforeAll(): Unit = {
      db.connect()

      while (!db.connection.isConnected) { Thread.sleep(10) }

      try {
        Await.result(db.connection.sendQuery(Queries.DROPDB.statement), 1.second)
        Await.result(db.connection.sendQuery(Queries.CREATEDB.statement), 1.second)
        Await.result(db.connection.sendQuery(Queries.DROP.statement), 1.second)
        Await.result(db.connection.sendQuery(Queries.CREATE.statement), 1.second)
      } catch {
        case e: Exception ⇒ println(s"Could not initialize database for tests ${e.getMessage}\n${e.getStackTrace.mkString("\n")}")
      }
    }

    override def afterAll(): Unit = {

      db.disconnect()
    }

    def withDatabase(testCode: Db ⇒ Any) {
      try {
        db.connect()
        testCode(db)
      } finally db.disconnect()
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

    object TX {
      val BEGIN = "begin"
      val COMMIT = "commit"
      val ROLLBACK = "rollback"
    }

    object TABLE {
      val name = "user"
      val fields = List("id", "username", "password", "login_count")
    }

    object INSERT {
      val username = "username"
      val password = "password"
      val loginCount = 0

      def statement: String = statement()
      def statement(id: String = "NULL", username: String = this.username, password: String = this.password, loginCount: Int = this.loginCount, email: Option[String] = None): String = {
        val eml = email.map(e ⇒ s", '${e}'").getOrElse("")
        s"""INSERT INTO mypipe.user values ($id, "$username", "$password", $loginCount $eml)"""
      }
    }

    object UPDATE {
      val username = "username2"
      val password = "password2"
      lazy val statement = s"""UPDATE mypipe.user set username = "$username", password = "$password", login_count = login_count + 1"""
    }

    object TRUNCATE {
      val statement = """TRUNCATE mypipe.user"""
    }

    object DELETE {
      val statement = """DELETE from mypipe.user"""
    }

    object CREATEDB {
      val statement = conf.getString("mypipe.test.database.create.db")
    }

    object DROPDB {
      val statement = conf.getString("mypipe.test.database.drop.db")
    }

    object CREATE {
      val statement = conf.getString("mypipe.test.database.create.table")
    }

    object DROP {
      val statement = conf.getString("mypipe.test.database.drop.table")
    }

    object ALTER {
      val statementAdd = conf.getString("mypipe.test.database.alter.add")
      val statementDrop = conf.getString("mypipe.test.database.alter.drop")
    }
  }

}

