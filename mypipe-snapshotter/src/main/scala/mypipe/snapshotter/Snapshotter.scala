package mypipe.snapshotter

import com.github.mauricio.async.db.mysql.MySQLConnection
import mypipe.api.consumer.{ BinaryLogConsumer, BinaryLogConsumerListener }
import mypipe.api.event.Mutation

import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import com.github.mauricio.async.db.{ Configuration, QueryResult, Connection }
import com.typesafe.config.ConfigFactory
import org.slf4j.LoggerFactory

import scala.concurrent.{ Future, Await }

object Snapshotter extends App {

  val log = LoggerFactory.getLogger(getClass)
  val conf = ConfigFactory.load()
  val Array(dbHost, dbPort, dbUsername, dbPassword, dbName) = conf.getString("mypipe.snapshotter.database.info").split(":")
  val tables = Seq("mypipe.user")

  val db = Db(dbHost, dbPort.toInt, dbUsername, dbPassword, dbName)
  implicit lazy val c: Connection = db.connection

  db.connect()

  while (!db.connection.isConnected) Thread.sleep(10)

  log.info(s"Connected to ${db.hostname}:${db.port}")

  val selects = MySQLSnapshotter.snapshotToSelects(MySQLSnapshotter.snapshot(tables))

  log.info("Fetched snapshot.")
  val selectConsumer = new SelectConsumer(dbUsername, dbHost, dbPassword, dbPort.toInt)
  val listener = new BinaryLogConsumerListener[SelectEvent, Nothing] {
    override def onMutation(consumer: BinaryLogConsumer[SelectEvent, Nothing], mutation: Mutation): Boolean = {
      log.info(s"Got mutation: $mutation")
      true
    }

    override def onMutation(consumer: BinaryLogConsumer[SelectEvent, Nothing], mutations: Seq[Mutation]): Boolean = {
      log.info(s"Got mutation: $mutations")
      true
    }
  }

  selectConsumer.registerListener(listener)

  sys.addShutdownHook({
    // TODO: expose this
    //selectConsumer.disconnect
    log.info(s"Disconnecting from ${db.hostname}:${db.port}")
    db.disconnect()
    log.info("Shutting down...")
  })

  log.info("Consumer setup done.")

  selectConsumer.handleEvents(Await.result(selects, 10.seconds))

  log.info("All events handled, exiting.")
}

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
