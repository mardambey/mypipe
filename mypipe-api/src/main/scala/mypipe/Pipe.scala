package mypipe

import scala.concurrent.duration._
import mypipe.mysql.{ BinlogFilePos, BinlogConsumerListener, BinlogConsumer }
import mypipe.api.{ Mutation, Log, Producer }
import akka.actor.{ Cancellable, ActorSystem }

class Pipe(id: String, consumers: List[BinlogConsumer], producer: Producer) {

  protected var CONSUMER_DISCONNECT_WAIT_SECS = 2
  protected val system = ActorSystem("mypipe")
  implicit val ec = system.dispatcher

  @volatile protected var _connected: Boolean = false
  protected var threads = List.empty[Thread]
  protected var flusher: Option[Cancellable] = None

  protected val listener = new BinlogConsumerListener() {

    override def onConnect(consumer: BinlogConsumer) {
      Log.info(s"Pipe $id connected!")

      _connected = true

      flusher = Some(system.scheduler.schedule(Conf.FLUSH_INTERVAL_SECS seconds,
        Conf.FLUSH_INTERVAL_SECS seconds) {
          Conf.binlogFilePosSave(consumer.hostname, consumer.port,
            BinlogFilePos(consumer.client.getBinlogFilename, consumer.client.getBinlogPosition),
            id)
          // TODO: if flush fails, stop and disconnect
          producer.flush
        })
    }

    override def onDisconnect(consumer: BinlogConsumer) {
      Log.info(s"Pipe $id disconnected!")
      _connected = false
      flusher.foreach(_.cancel())
      Conf.binlogFilePosSave(
        consumer.hostname,
        consumer.port,
        BinlogFilePos(consumer.client.getBinlogFilename, consumer.client.getBinlogPosition),
        id)
      producer.flush
    }

    override def onMutation(consumer: BinlogConsumer, mutation: Mutation[_]): Boolean = {
      producer.queue(mutation)
    }

    override def onMutation(consumer: BinlogConsumer, mutations: Seq[Mutation[_]]): Boolean = {
      producer.queueList(mutations.toList)
    }
  }

  def isConnected = _connected

  def connect() {

    if (threads.size > 0) {

      Log.warning("Attempting to reconnect pipe while already connected, aborting!")

    } else {

      threads = consumers.map(c ⇒ {
        c.registerListener(listener)
        val t = new Thread() {
          override def run() {
            Log.info(s"Connecting pipe between $c -> $producer")
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
        Log.info(s"Disconnecting pipe between ${c} -> ${producer}")
        c.disconnect()
        t.join(CONSUMER_DISCONNECT_WAIT_SECS * 1000)
      } catch {
        case e: Exception ⇒ Log.severe(s"Caught exception while trying to disconnect from ${c.hostname}:${c.port} at binlog position ${c.binlogFileAndPos}.")
      }
    }
  }

  override def toString(): String = id
}
