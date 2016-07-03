package mypipe.snapshotter

import com.github.mauricio.async.db.mysql.MySQLConnection
import mypipe.api.consumer.BinaryLogConsumer
import mypipe.api.producer.Producer
import mypipe.runner.PipeRunnerUtil
import scopt.OptionParser
import mypipe.pipe.Pipe

import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import com.github.mauricio.async.db.{Configuration, Connection}
import com.typesafe.config.{Config, ConfigFactory}
import org.slf4j.LoggerFactory

import scala.concurrent.Await

object Snapshotter extends App {

  private[Snapshotter] case class CmdLineOptions(tables: Seq[String] = Seq.empty, noTransaction: Boolean = false, numSplits: Int = 5, splitLimit: Int = 100, whereClause: String = "")

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
    opt[Unit]('x', "no-transaction") optional () action { (_, c) ⇒
      c.copy(noTransaction = true)
    } text "do not perform the snapshot using a consistent read in a transaction"
    opt[Int]('n', "numSplits") optional () valueName "5" action { (x, c) ⇒
      c.copy(numSplits = x)
    } text "number of splits to divide to table into" validate (t ⇒ if (t > 0) success else failure("the number of splits must be at least 1"))
    opt[Int]('l', "splitLimit") optional () valueName "100" action { (x, c) ⇒
      c.copy(splitLimit = x)
    } text "maximum possible number of splits to divide to table into" validate (t ⇒ if (t > 0 && t <= 100) success else failure("the maximum number of splits must be between 1 and 100"))
    opt[String]('w', "whereClause") optional () action { (x, c) ⇒
      c.copy(whereClause = x.trim)
    } text "additional WHERE clause parameters that will be ANDed to the existing ones" validate (t ⇒ if (t.trim.length > 0) success else failure("WHERE clause must be non-empty"))
  }

  val cmdLineOptions = parser.parse(args, CmdLineOptions())

  cmdLineOptions match {
    case Some(cfg) ⇒
      snapshot(cfg.tables, cfg.noTransaction, cfg.numSplits, cfg.splitLimit, cfg.whereClause)
    case None ⇒
      log.debug(s"Could not parser parameters, aborting: ${args.mkString(" ")}")
  }

  private def snapshot(tables: Seq[String], noTransaction: Boolean, numSplits: Int, splitLimit: Int, whereClause: String): Unit = {

    db.connect()

    while (!db.connection.isConnected) Thread.sleep(10)

    log.info(s"Connected to ${db.hostname}:${db.port} (withTransaction=${!noTransaction}, numSplits=$numSplits, splitLimit=$splitLimit, additionalWhereClause=$whereClause)")

    lazy val producers: Map[String, Option[Class[Producer]]] = PipeRunnerUtil.loadProducerClasses(conf, "mypipe.snapshotter.producers")
    lazy val consumers: Seq[(String, Config, Option[Class[BinaryLogConsumer[_]]])] = PipeRunnerUtil.loadConsumerConfigs(conf, "mypipe.snapshotter.consumers")
    lazy val pipes: Seq[Pipe[_]] = PipeRunnerUtil.createPipes(conf, "mypipe.snapshotter.pipes", producers, consumers)

    if (pipes.length != 1) {
      throw new Exception("Exactly 1 pipe should be configured for the snapshotter.")
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

    log.info(s"Snapshotting table ${tables.head}")

    val snapshotF = MySQLSnapshotter.snapshot(
      db = tables.head.split("\\.").head,
      table = tables.head.split("\\.")(1),
      numSplits = numSplits,
      splitLimit = splitLimit,
      splitByColumnName = None,
      selectQuery = None,
      whereClause = if (whereClause.length > 0) Some(whereClause) else None,
      eventHandler = { events ⇒

        log.info(s"Fetched snapshot for ${tables.head} (${events.length} rows)")

        pipe.consumer.asInstanceOf[SelectConsumer].handleEvents(events)

        log.info(s"Snapshot for ${tables.head} (${events.length} rows) converted to events.")
      }
    )

    log.info("Waiting for snapshots to finish...")
    Await.result(snapshotF, Duration.Inf)
    log.info("Done waiting for snapshots to finish.")
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
