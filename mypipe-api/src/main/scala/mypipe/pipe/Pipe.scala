package mypipe.pipe

import akka.actor.{ ActorSystem, Cancellable }
import mypipe.api.consumer.{ BinaryLogConsumer, BinaryLogConsumerListener }
import mypipe.api.event.{ AlterEvent, Mutation }
import mypipe.api.Conf
import mypipe.api.producer.Producer
import mypipe.mysql._
import org.slf4j.LoggerFactory

import com.github.shyiko.mysql.binlog.event.{ Event ⇒ MEvent }

import scala.concurrent.duration._

class Pipe(id: String, consumers: List[MySQLBinaryLogConsumer], producer: Producer) {

  protected val log = LoggerFactory.getLogger(getClass)
  protected var CONSUMER_DISCONNECT_WAIT_SECS = 2
  protected val system = ActorSystem("mypipe")
  implicit val ec = system.dispatcher

  @volatile protected var _connected: Boolean = false
  protected var threads = List.empty[Thread]
  protected var flusher: Option[Cancellable] = None

  protected val listener = new BinaryLogConsumerListener[MEvent, BinaryLogFilePosition]() {

    override def onConnect(consumer: BinaryLogConsumer[MEvent, BinaryLogFilePosition]) {
      log.info(s"Pipe $id connected!")

      _connected = true

      flusher = Some(system.scheduler.schedule(Conf.FLUSH_INTERVAL_SECS.seconds,
        Conf.FLUSH_INTERVAL_SECS.seconds) {
          Conf.binlogSaveFilePosition(consumer.id,
            consumer.getBinaryLogPosition.get,
            id)
          // TODO: if flush fails, stop and disconnect
          producer.flush()
        })
    }

    override def onDisconnect(consumer: BinaryLogConsumer[MEvent, BinaryLogFilePosition]) {
      log.info(s"Pipe $id disconnected!")
      _connected = false
      flusher.foreach(_.cancel())
      Conf.binlogSaveFilePosition(
        consumer.id,
        consumer.getBinaryLogPosition.get,
        id)
      producer.flush()
    }

    override def onMutation(consumer: BinaryLogConsumer[MEvent, BinaryLogFilePosition], mutation: Mutation): Boolean = {
      producer.queue(mutation)
    }

    override def onMutation(consumer: BinaryLogConsumer[MEvent, BinaryLogFilePosition], mutations: Seq[Mutation]): Boolean = {
      producer.queueList(mutations.toList)
    }

    override def onTableAlter(consumer: BinaryLogConsumer[MEvent, BinaryLogFilePosition], event: AlterEvent): Boolean = {
      producer.handleAlter(event)
    }
  }

  def isConnected = _connected

  def connect() {

    if (threads.nonEmpty) {

      log.warn("Attempting to reconnect pipe while already connected, aborting!")

    } else {

      threads = consumers.map(c ⇒ {
        c.registerListener(listener)
        val t = new Thread() {
          override def run() {
            log.info(s"Connecting pipe between $c -> $producer")
            c.connect()
          }
        }

        t.start()
        t
      })
    }
  }

  def disconnect() {
    for (
      c ← consumers;
      t ← threads
    ) {
      try {
        log.info(s"Disconnecting pipe between $c -> $producer")
        c.disconnect()
        t.join(CONSUMER_DISCONNECT_WAIT_SECS * 1000)
      } catch {
        case e: Exception ⇒ log.error(s"Caught exception while trying to disconnect from $c.id at binlog position $c.getBinaryLogPosition.")
      }
    }
  }

  override def toString: String = id
}

