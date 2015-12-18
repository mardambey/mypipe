package mypipe.pipe

import akka.actor.{ ActorSystem, Cancellable }
import mypipe.api.consumer.{ BinaryLogConsumer, BinaryLogConsumerListener }
import mypipe.api.event.{ AlterEvent, Mutation }
import mypipe.api.Conf
import mypipe.api.producer.Producer
import mypipe.util.Enum
import org.slf4j.LoggerFactory

import scala.concurrent.Future
import scala.concurrent.duration._

case class Pipe[BinaryLogEvent](id: String, consumer: BinaryLogConsumer[BinaryLogEvent], producer: Producer) {

  object State extends Enum {

    sealed trait EnumVal extends Value

    val STOPPED = new EnumVal {
      val value = 0
    }

    val STARTING = new EnumVal {
      val value = 1
    }

    val STARTED = new EnumVal {
      val value = 2
    }

    val CRASHED = new EnumVal {
      val value = 3
    }
  }

  protected val log = LoggerFactory.getLogger(getClass)
  protected var state = State.STOPPED
  protected var CONSUMER_DISCONNECT_WAIT_SECS = 2
  protected val system = ActorSystem("mypipe")
  implicit val ec = system.dispatcher

  @volatile protected var _connected: Boolean = false
  protected var flusher: Option[Cancellable] = None
  protected val listener = new BinaryLogConsumerListener[BinaryLogEvent]() {

    override def onStart(consumer: BinaryLogConsumer[BinaryLogEvent]) {
      log.info(s"Pipe $id connected!")

      state = State.STARTED
      _connected = true

      flusher = Some(system.scheduler.schedule(Conf.FLUSH_INTERVAL_SECS.seconds,
        Conf.FLUSH_INTERVAL_SECS.seconds)(saveBinaryLogPosition(consumer)))
    }

    private def saveBinaryLogPosition(consumer: BinaryLogConsumer[BinaryLogEvent]) {
      val binlogPos = consumer.getBinaryLogPosition
      if (producer.flush() && binlogPos.isDefined) {
        binlogSaveFilePosition(consumer, id)
      } else {
        if (binlogPos.isDefined)
          log.error(s"Producer ($producer) failed to flush, not saving binary log position: $binlogPos for consumer $consumer.")
        else
          log.error(s"Producer ($producer) flushed, but encountered no binary log position to save for consumer $consumer.")
      }
    }

    private def binlogSaveFilePosition(consumer: BinaryLogConsumer[_], pipe: String): Boolean = {
      val fileName = Conf.binlogGetStatusFilename(consumer.id, pipe)
      log.info(s"Saving binlog position for pipe $pipe/${consumer.id} -> ${consumer.getBinaryLogPosition}")
      Conf.binlogSaveFilePositionToFile(consumer, fileName)
    }

    override def onStop(consumer: BinaryLogConsumer[BinaryLogEvent]) {
      log.info(s"Pipe $id disconnected!")
      _connected = false
      flusher.foreach(_.cancel())
      saveBinaryLogPosition(consumer)
      state = State.STOPPED
    }

    override def onMutation(consumer: BinaryLogConsumer[BinaryLogEvent], mutation: Mutation): Boolean = {
      producer.queue(mutation)
    }

    override def onMutation(consumer: BinaryLogConsumer[BinaryLogEvent], mutations: Seq[Mutation]): Boolean = {
      producer.queueList(mutations.toList)
    }

    override def onTableAlter(consumer: BinaryLogConsumer[BinaryLogEvent], event: AlterEvent): Boolean = {
      producer.handleAlter(event)
    }
  }

  def isConnected = _connected

  def connect(): Future[Boolean] = {

    if (state != State.STOPPED) {
      val error = "Attempting to reconnect pipe while already connected, aborting!"
      log.error(error)
      Future.failed(new Exception(error))
    } else {
      state = State.STARTING
      consumer.registerListener(listener)
      log.info(s"Connecting pipe between $consumer -> $producer")
      consumer.start()
    }
  }

  def disconnect() {
    if (state != State.STOPPED && state != State.CRASHED) {
      try {
        log.info(s"Disconnecting pipe between $consumer -> $producer")
        consumer.stop()
        state = State.STOPPED
      } catch {
        case e: Exception ⇒
          state = State.CRASHED
          log.error(s"Caught exception while trying to disconnect from ${consumer.id} at binlog position ${consumer.getBinaryLogPosition}.")
      }
    }
  }

  override def toString: String = id
}

