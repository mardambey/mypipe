package mypipe

import com.github.shyiko.mysql.binlog.BinaryLogClient
import com.github.shyiko.mysql.binlog.BinaryLogClient.{ LifecycleListener, EventListener }
import com.github.shyiko.mysql.binlog.event._
import com.github.shyiko.mysql.binlog.event.EventType._
import com.typesafe.config.ConfigFactory

import scala.collection.JavaConverters._
import java.io.{ PrintWriter, File }
import akka.actor.{ Props, ActorSystem, Actor }
import akka.pattern.ask
import scala.concurrent.duration._
import java.util.logging.{ FileHandler, Level, Logger, ConsoleHandler }
import java.io.Serializable
import scala.collection.mutable
import scala.concurrent.Await
import scala.Some
import com.netflix.astyanax.MutationBatch

object Log {
  val log = Logger.getLogger(getClass.getName)
  log.setUseParentHandlers(false)
  log.setLevel(Level.ALL)

  val handlers = List(new ConsoleHandler, new FileHandler(s"${Conf.LOGDIR}/mypipe.log", true /* append */ ))

  handlers.foreach(h ⇒ {
    h.setFormatter(new LogFormatter())
    log.addHandler(h)
  })

  sys.addShutdownHook({
    handlers.foreach(_.close())
  })

  def info(str: String) = log.info(str)
  def warning(str: String) = log.warning(str)
  def severe(str: String) = log.severe(str)
  def fine(str: String) = log.fine(str)
  def finer(str: String) = log.finer(str)
  def finest(str: String) = log.finest(str)

  import java.io.PrintWriter
  import java.io.StringWriter
  import java.util.Date
  import java.util.logging.Formatter
  import java.util.logging.LogRecord

  private class LogFormatter extends Formatter {

    val LINE_SEPARATOR = System.getProperty("line.separator")

    override def format(record: LogRecord): String = {
      val sb = new StringBuilder()

      sb.append(new Date(record.getMillis()))
        .append(" [")
        .append(record.getLevel().getLocalizedName())
        .append("] ")
        .append(formatMessage(record))
        .append(LINE_SEPARATOR)

      if (record.getThrown() != null) {
        try {
          val sw = new StringWriter()
          val pw = new PrintWriter(sw)
          record.getThrown().printStackTrace(pw)
          pw.close()
          sb.append(sw.toString())
        } catch {
          case t: Throwable ⇒ // ignore
        }
      }

      return sb.toString()
    }
  }
}

trait Producer {
  def queue(mutation: Mutation[_])
  def queueList(mutation: List[Mutation[_]])
  def flush()
}

abstract class Mutation[T](val db: String, val table: String, val rows: T) {
  def execute()
}

case class InsertMutation(
  override val db: String,
  override val table: String,
  override val rows: List[Array[Serializable]])
    extends Mutation[List[Array[Serializable]]](db, table, rows) {

  def execute() {
    Log.info(s"executing insert mutation")
  }
}

case class UpdateMutation(
  override val db: String,
  override val table: String,
  override val rows: List[(Array[Serializable], Array[Serializable])])
    extends Mutation[List[(Array[Serializable], Array[Serializable])]](db, table, rows) {

  def execute() {
    Log.info(s"executing update mutation")
  }
}

case class DeleteMutation(
  override val db: String,
  override val table: String,
  override val rows: List[Array[Serializable]])
    extends Mutation[List[Array[Serializable]]](db, table, rows) {

  def execute() {
    Log.info(s"executing delete mutation")
  }
}

case class Queue(mutation: Mutation[_])
case class QueueList(mutations: List[Mutation[_]])
case object Flush

class CassandraBatchWriter extends Actor {

  implicit val ec = context.dispatcher
  val mutations = scala.collection.mutable.ListBuffer[MutationBatch]()
  val cancellable =
    context.system.scheduler.schedule(Conf.CASSANDRA_FLUSH_INTERVAL_SECS seconds,
      Conf.CASSANDRA_FLUSH_INTERVAL_SECS seconds,
      self,
      Flush)

  def receive = {
    case Queue(mutation)      ⇒ mutations += map(mutation)
    case QueueList(mutationz) ⇒ mutations += map(mutationz)
    case Flush                ⇒ sender ! flush()
  }

  def flush(): Boolean = {
    Log.warning(s"TODO: flush ${mutations.size} mutations.")

    // TODO: flush
    mutations.clear()
    true
  }

  def map(mutation: Mutation[_]): MutationBatch = {
    map(mutation, null)
    null
  }

  def map(mutations: List[Mutation[_]]): MutationBatch = {
    mutations.foreach(m ⇒ map(m, null))
    null
  }

  def map(mutation: Mutation[_], mutationBatch: MutationBatch) {

    mutation match {

      case i: InsertMutation ⇒ {
        i.rows.foreach(row ⇒ {
          println(s"""InsertMutation: row=${row.getClass.getSimpleName} values=${row.mkString(",")}""")
        })
      }

      case u: UpdateMutation ⇒ {
        u.rows.foreach(row ⇒ {
          val old = row._1
          val cur = row._2

          println(s"""UpdateMutation: old=${old.mkString(",")}, cur=${cur.mkString(",")}""")
        })
      }

      case d: DeleteMutation ⇒ {
        d.rows.foreach(row ⇒ {
          println(s"""DeleteMutation: row=${row.getClass.getSimpleName} values=${row.mkString(",")}""")
        })
      }
    }
  }
}

object CassandraBatchWriter {
  def props(): Props = Props(classOf[CassandraBatchWriter])
}

case class CassandraProducer extends Producer {

  val system = ActorSystem("mypipe")
  val worker = system.actorOf(CassandraBatchWriter.props(), "CassandraBatchWriterActor")

  def queue(mutation: Mutation[_]) {
    worker ! Queue(mutation)
  }

  def queueList(mutations: List[Mutation[_]]) {
    worker ! QueueList(mutations)
  }

  def flush() {
    val future = worker.ask(Flush)(Conf.SHUTDOWN_FLUSH_WAIT_SECS seconds)
    val result = Await.result(future, Conf.SHUTDOWN_FLUSH_WAIT_SECS seconds).asInstanceOf[Boolean]
  }
}

object Conf {

  val conf = ConfigFactory.load()
  val sources = conf.getStringList("mypipe.sources")
  val DATADIR = conf.getString("mypipe.datadir")
  val LOGDIR = conf.getString("mypipe.logdir")
  val SHUTDOWN_FLUSH_WAIT_SECS = conf.getInt("mypipe.shutdown-wait-time-seconds")
  val CASSANDRA_FLUSH_INTERVAL_SECS = conf.getInt("mypipe.cassandra-flush-interval-seconds")
  val GROUP_EVENTS_BY_TX = conf.getBoolean("mypipe.group-events-by-tx")

  try {
    new File(DATADIR).mkdirs()
    new File(LOGDIR).mkdirs()
  } catch {
    case e: Exception ⇒ println(s"Error while creating data and log dir ${DATADIR}, ${LOGDIR}: ${e.getMessage}")
  }

  def binlogStatusFile(hostname: String, port: Int): String = {
    s"$DATADIR/$hostname-$port.pos"
  }

  def binlogFilePos(hostname: String, port: Int): Option[BinlogFilePos] = {
    try {

      val statusFile = binlogStatusFile(hostname, port)
      val filePos = scala.io.Source.fromFile(statusFile).getLines().mkString.split(":")
      Some(BinlogFilePos(filePos(0), filePos(1).toLong))

    } catch {
      case e: Exception ⇒ None
    }
  }

  def binlogFilePosSave(hostname: String, port: Int, filePos: BinlogFilePos) {
    Log.info(s"Saving binlog position for $hostname:$port => $filePos")
    val fileName = binlogStatusFile(hostname, port)
    val file = new File(fileName)
    val writer = new PrintWriter(file)
    writer.write(s"${filePos.filename}:${filePos.pos}")
    writer.close()
  }
}

case class BinlogFilePos(filename: String, pos: Long) {
  override def toString(): String = s"$filename:$pos"
}

object BinlogFilePos {
  val current = BinlogFilePos("", 0)
}

case class BinlogConsumer(hostname: String, port: Int, username: String, password: String, binlogFileAndPos: BinlogFilePos) {

  val tablesById = scala.collection.mutable.HashMap[Long, TableMapEventData]()
  var transactionInProgress = false
  val groupEventsByTx = Conf.GROUP_EVENTS_BY_TX
  val producers = new mutable.HashSet[Producer]()
  val txQueue = new mutable.ListBuffer[Event]
  val client = new BinaryLogClient(hostname, port, username, password);

  client.registerEventListener(new EventListener() {

    override def onEvent(event: Event) {

      val eventType = event.getHeader().asInstanceOf[EventHeader].getEventType()

      eventType match {
        case TABLE_MAP ⇒ {
          val tableMapEventData: TableMapEventData = event.getData();
          tablesById.put(tableMapEventData.getTableId(), tableMapEventData)
        }

        case e: EventType if isMutation(eventType) == true ⇒ {
          if (groupEventsByTx) {
            txQueue += event
          } else {
            producers foreach (p ⇒ p.queue(createMutation(event)))
          }
        }

        case QUERY ⇒ {
          if (groupEventsByTx) {
            val queryEventData: QueryEventData = event.getData()
            val query = queryEventData.getSql()
            if (groupEventsByTx) {
              if ("BEGIN".equals(query)) {
                transactionInProgress = true
              } else if ("COMMIT".equals(query)) {
                commit()
              } else if ("ROLLBACK".equals(query)) {
                rollback()
              }
            }
          }
        }
        case XID ⇒ {
          if (groupEventsByTx) {
            commit()
          }
        }
        case _ ⇒ println(s"ignored ${eventType}")
      }
    }

    def rollback() {
      txQueue.clear
      transactionInProgress = false
    }

    def commit() {
      val mutations = txQueue.map(createMutation(_))
      producers foreach (p ⇒ p.queueList(mutations.toList))
      txQueue.clear
      transactionInProgress = false
    }
  })

  if (binlogFileAndPos != BinlogFilePos.current) {
    Log.info(s"Resuming binlog consumption from file=${binlogFileAndPos.filename} pos=${binlogFileAndPos.pos} for $hostname:$port")
    client.setBinlogFilename(binlogFileAndPos.filename)
    client.setBinlogPosition(binlogFileAndPos.pos)
  } else {
    Log.info(s"Using current master binlog position for consuming from $hostname:$port")
  }

  client.registerLifecycleListener(new LifecycleListener {
    override def onDisconnect(client: BinaryLogClient): Unit = {
      Conf.binlogFilePosSave(hostname, port, BinlogFilePos(client.getBinlogFilename, client.getBinlogPosition))
    }

    override def onEventDeserializationFailure(client: BinaryLogClient, ex: Exception) {}
    override def onConnect(client: BinaryLogClient) {}
    override def onCommunicationFailure(client: BinaryLogClient, ex: Exception) {}
  })

  def connect() {
    client.connect()
  }

  def disconnect() {
    client.disconnect()
    producers foreach (p ⇒ p.flush)
  }

  def registerProducer(producer: Producer) {
    producers += producer
  }

  def isMutation(eventType: EventType): Boolean = eventType match {
    case PRE_GA_WRITE_ROWS | WRITE_ROWS | EXT_WRITE_ROWS |
      PRE_GA_UPDATE_ROWS | UPDATE_ROWS | EXT_UPDATE_ROWS |
      PRE_GA_DELETE_ROWS | DELETE_ROWS | EXT_DELETE_ROWS ⇒ true
    case _ ⇒ false
  }

  def createMutation(event: Event): Mutation[_] = event.getHeader().asInstanceOf[EventHeader].getEventType() match {
    case PRE_GA_WRITE_ROWS | WRITE_ROWS | EXT_WRITE_ROWS ⇒ {
      val evData = event.getData[WriteRowsEventData]()
      val tableData = tablesById.get(evData.getTableId).get
      InsertMutation(tableData.getDatabase(), tableData.getTable(), evData.getRows().asScala.toList)
    }

    case PRE_GA_UPDATE_ROWS | UPDATE_ROWS | EXT_UPDATE_ROWS ⇒ {
      val evData = event.getData[UpdateRowsEventData]()
      val tableData = tablesById.get(evData.getTableId).get
      val rows = evData.getRows().asScala.toList.map(row ⇒ {
        (row.getKey, row.getValue)
      })

      UpdateMutation(tableData.getDatabase(), tableData.getTable(), rows)
    }
    case PRE_GA_DELETE_ROWS | DELETE_ROWS | EXT_DELETE_ROWS ⇒ {
      val evData = event.getData[DeleteRowsEventData]()
      val tableData = tablesById.get(evData.getTableId).get
      DeleteMutation(tableData.getDatabase(), tableData.getTable(), evData.getRows().asScala.toList)
    }
  }
}

class HostPortUserPass(val host: String, val port: Int, val user: String, val password: String)
object HostPortUserPass {

  def apply(hostPortUserPass: String) = {
    val params = hostPortUserPass.split(":")
    new HostPortUserPass(params(0), params(1).toInt, params(2), params(3))
  }
}

object Mypipe extends App {

  val producer = CassandraProducer()

  val consumers = Conf.sources.asScala.map(
    source ⇒ {
      val params = HostPortUserPass(source)
      val filePos = Conf.binlogFilePos(params.host, params.port).getOrElse(BinlogFilePos.current)
      val consumer = BinlogConsumer(params.host, params.port, params.user, params.password, filePos)
      consumer.registerProducer(producer)
      consumer
    })

  sys.addShutdownHook({
    Log.info("Shutting down...")
    consumers.foreach(c ⇒ c.disconnect())
  })

  val threads = consumers.map(c ⇒ new Thread() { override def run() { c.connect() } })
  threads.foreach(_.start())
  threads.foreach(_.join())
}
