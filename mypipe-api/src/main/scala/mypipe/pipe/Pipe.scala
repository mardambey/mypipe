package mypipe.pipe

import akka.actor.{ ActorSystem, Cancellable }
import mypipe.api.consumer.{ BinaryLogConsumer, BinaryLogConsumerListener }
import mypipe.api.event.{ AlterEvent, Mutation }
import mypipe.api.Conf
import mypipe.api.producer.Producer
import mypipe.mysql._
import mypipe.util.Enum
import org.slf4j.LoggerFactory

import scala.concurrent.Future
import scala.concurrent.duration._

case class Pipe[BinaryLogEvent, BinaryLogPosition](id: String, consumer: BinaryLogConsumer[BinaryLogEvent, BinaryLogPosition], producer: Producer) {

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
  protected val listener = new BinaryLogConsumerListener[BinaryLogEvent, BinaryLogPosition]() {

    override def onStart(consumer: BinaryLogConsumer[BinaryLogEvent, BinaryLogPosition]) {
      log.info(s"Pipe $id connected!")

      state = State.STARTED
      _connected = true

      flusher = Some(system.scheduler.schedule(Conf.FLUSH_INTERVAL_SECS.seconds,
        Conf.FLUSH_INTERVAL_SECS.seconds)(saveBinaryLogPosition(consumer)))
    }

    private def saveBinaryLogPosition(consumer: BinaryLogConsumer[BinaryLogEvent, BinaryLogPosition]) {
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

    override def onStop(consumer: BinaryLogConsumer[BinaryLogEvent, BinaryLogPosition]) {
      log.info(s"Pipe $id disconnected!")
      _connected = false
      flusher.foreach(_.cancel())
      saveBinaryLogPosition(consumer)
      state = State.STOPPED
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

