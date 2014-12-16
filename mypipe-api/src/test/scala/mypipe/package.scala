import akka.actor.ActorSystem
import com.github.mauricio.async.db.{ Configuration, Connection }
import com.github.mauricio.async.db.mysql.MySQLConnection
import com.typesafe.config.ConfigFactory
import scala.concurrent.duration._
import scala.concurrent.Await

package object mypipe {

  case class Db(hostname: String, port: Int, username: String, password: String, dbName: String) {

    private val configuration = new Configuration(username, hostname, port, Some(password), Some(dbName))
    var connection: Connection = _

    def connect(): Unit = connect(timeoutMillis = 5000)
    def connect(timeoutMillis: Int) {
      connection = new MySQLConnection(configuration)
      val future = connection.connect
      Await.result(future, timeoutMillis.millis)
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

  trait DatabaseSpec extends ConfigSpec {

    val db = Db(
      Queries.DATABASE.host,
      Queries.DATABASE.port,
      Queries.DATABASE.username,
      Queries.DATABASE.password,
      Queries.DATABASE.name)

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

    object TABLE {
      val name = "user"
      val fields = List("id", "username", "password", "login_count")
    }

    object INSERT {
      val username = "username"
      val password = "password"
      val loginCount = 0

      def statement: String = statement()
      def statement(id: String = "NULL", username: String = this.username, password: String = this.password, loginCount: Int = this.loginCount): String =
        s"""INSERT INTO user (id, username, password, login_count) values ($id, "$username", "$password", $loginCount)"""
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

    object ALTER {
      val statementAdd = conf.getString("mypipe.test.database.alter.add")
      val statementDrop = conf.getString("mypipe.test.database.alter.drop")
    }
  }

}

