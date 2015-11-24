package mypipe.snapshotter

import com.github.mauricio.async.db.mysql.MySQLConnection
import mypipe.api.HostPortUserPass
import mypipe.api.consumer.BinaryLogConsumer
import mypipe.api.producer.Producer
import mypipe.runner.PipeRunnerUtil
import scopt.OptionParser
import mypipe.pipe.Pipe

import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import com.github.mauricio.async.db.{ Configuration, Connection }
import com.typesafe.config.ConfigFactory
import org.slf4j.LoggerFactory

import scala.concurrent.Await

object Snapshotter extends App {

  private[Snapshotter] case class CmdLineOptions(tables: Seq[String] = Seq.empty)

  val log = LoggerFactory.getLogger(getClass)
  val conf = ConfigFactory.load()
  val Array(dbHost, dbPort, dbUsername, dbPassword, dbName) = conf.getString("mypipe.snapshotter.database.info").split(":")
  val db = Db(dbHost, dbPort.toInt, dbUsername, dbPassword, dbName)
  implicit lazy val c: Connection = db.connection
  val parser = new OptionParser[CmdLineOptions]("mypipe-snapshotter") {
    head("mypipe-snapshotter", "1.0")
    opt[Seq[String]]('t', "tables") required () valueName "<db1.table1>,<db1.table2>,<db2.table1>..." action { (x, c) ⇒
      c.copy(tables = x)
    } text "tables to include, format is: database.table" validate (t ⇒ if (t.nonEmpty) success else failure("at least one table must be given"))
  }

  val cmdLineOptions = parser.parse(args, CmdLineOptions())

  cmdLineOptions match {
    case Some(c) ⇒
      val tables = c.tables
      snapshot(tables)
    case None ⇒
      log.debug(s"Could not parser parameters, aborting: ${args.mkString(" ")}")
  }

  private def snapshot(tables: Seq[String]): Unit = {

    db.connect()

    while (!db.connection.isConnected) Thread.sleep(10)

    log.info(s"Connected to ${db.hostname}:${db.port}.")

    val selects = MySQLSnapshotter.snapshotToSelects(MySQLSnapshotter.snapshot(tables))

    log.info("Fetched snapshot.")

    lazy val producers: Map[String, Option[Class[Producer]]] = PipeRunnerUtil.loadProducerClasses(conf, "mypipe.snapshotter.producers")
    lazy val consumers: Seq[(String, HostPortUserPass, Option[Class[BinaryLogConsumer[_, _]]])] = PipeRunnerUtil.loadConsumerConfigs(conf, "mypipe.snapshotter.consumers")
    lazy val pipes: Seq[Pipe[_, _]] = PipeRunnerUtil.createPipes(conf, "mypipe.snapshotter.pipes", producers, consumers)

    if (pipes.length != 1) {
      throw new Exception("Exactly 1 pipe should be configured configured for snapshotter.")
    }

    if (!pipes.head.consumer.isInstanceOf[SelectConsumer]) {
      throw new Exception(s"Snapshotter requires a SelectConsumer, ${pipes.head.consumer.getClass.getName} found instead.")
    }

    val pipe = pipes.head

    sys.addShutdownHook({
      pipe.disconnect()
      log.info(s"Disconnecting from ${db.hostname}:${db.port}.")
      db.disconnect()
      log.info("Shutting down...")
    })

    log.info("Consumer setup done.")

    pipe.connect()
    pipe.consumer.asInstanceOf[SelectConsumer].handleEvents(Await.result(selects, 10.seconds))

    log.info("All events handled, safe to shut down.")
  }
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
