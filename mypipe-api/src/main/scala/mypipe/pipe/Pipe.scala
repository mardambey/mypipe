package mypipe.pipe

import akka.actor.{ ActorSystem, Cancellable }
import mypipe.api.consumer.{ BinaryLogConsumer, BinaryLogConsumerListener }
import mypipe.api.event.{ AlterEvent, Mutation }
import mypipe.api.Conf
import mypipe.api.producer.Producer
import mypipe.mysql._
import org.slf4j.LoggerFactory

import scala.concurrent.duration._

class Pipe[BinaryLogEvent, BinaryLogPosition](id: String, consumers: List[BinaryLogConsumer[BinaryLogEvent, BinaryLogPosition]], producer: Producer) {

  protected val log = LoggerFactory.getLogger(getClass)
  protected var CONSUMER_DISCONNECT_WAIT_SECS = 2
  protected val system = ActorSystem("mypipe")
  implicit val ec = system.dispatcher

  @volatile protected var _connected: Boolean = false
  protected var threads = List.empty[Thread]
  protected var flusher: Option[Cancellable] = None

  protected val listener = new BinaryLogConsumerListener[BinaryLogEvent, BinaryLogPosition]() {

    override def onConnect(consumer: BinaryLogConsumer[BinaryLogEvent, BinaryLogPosition]) {
      log.info(s"Pipe $id connected!")

      _connected = true

      flusher = Some(system.scheduler.schedule(Conf.FLUSH_INTERVAL_SECS.seconds,
        Conf.FLUSH_INTERVAL_SECS.seconds)(saveBinaryLogPosition(consumer)))
    }

    private def saveBinaryLogPosition(consumer: BinaryLogConsumer[BinaryLogEvent, BinaryLogPosition]) {
      log.info(s"saveBinaryLogPosition: $consumer")
      val binlogPos = consumer.getBinaryLogPosition
      // TODO: we should be able to generically save the binary log position
      if (producer.flush() && binlogPos.isDefined && binlogPos.get.isInstanceOf[BinaryLogFilePosition]) {
        Conf.binlogSaveFilePosition(consumer.id, binlogPos.get.asInstanceOf[BinaryLogFilePosition], id)
      } else {
        if (binlogPos.isDefined)
          log.error(s"Producer ($producer) failed to flush, not saving binary log position: $binlogPos for consumer $consumer.")
        else
          log.error(s"Producer ($producer) flushed, but encountered no binary log position to save for consumer $consumer.")
      }
    }

    override def onDisconnect(consumer: BinaryLogConsumer[BinaryLogEvent, BinaryLogPosition]) {
      log.info(s"Pipe $id disconnected!")
      _connected = false
      flusher.foreach(_.cancel())
      saveBinaryLogPosition(consumer)
    }

    override def onMutation(consumer: BinaryLogConsumer[BinaryLogEvent, BinaryLogPosition], mutation: Mutation): Boolean = {
      producer.queue(mutation)
    }

    override def onMutation(consumer: BinaryLogConsumer[BinaryLogEvent, BinaryLogPosition], mutations: Seq[Mutation]): Boolean = {
      producer.queueList(mutations.toList)
    }

    override def onTableAlter(consumer: BinaryLogConsumer[BinaryLogEvent, BinaryLogPosition], event: AlterEvent): Boolean = {
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
            c.start()
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
        c.stop()
        t.join(CONSUMER_DISCONNECT_WAIT_SECS * 1000)
      } catch {
        case e: Exception ⇒ log.error(s"Caught exception while trying to disconnect from $c.id at binlog position $c.getBinaryLogPosition.")
      }
    }
  }

  override def toString: String = id
}

