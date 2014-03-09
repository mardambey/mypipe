package mypipe.api

import java.io.Serializable
import com.github.shyiko.mysql.binlog.event.TableMapEventData
import com.github.shyiko.mysql.binlog.event.deserialization.{ ColumnType ⇒ MColumnType }
import mypipe.mysql.{ Listener, BinlogConsumer }
import com.typesafe.config.Config

abstract class Mapping {
  def map(mutation: InsertMutation) {}
  def map(mutation: UpdateMutation) {}
  def map(mutation: DeleteMutation) {}
}

abstract class Producer(mappings: List[Mapping], config: Config) {
  def queue(mutation: Mutation[_])
  def queueList(mutation: List[Mutation[_]])
  def flush()
}

object ColumnMetadata {
  type ColumnType = MColumnType

  def typeByCode(code: Int): ColumnType = MColumnType.byCode(code)
}

case class PrimaryKey(columns: List[ColumnMetadata])
case class ColumnMetadata(name: String, colType: ColumnMetadata.ColumnType, isPrimaryKey: Boolean)
case class Row(table: Table, columns: Map[String, Column])
case class Table(id: java.lang.Long, name: String, db: String, evData: TableMapEventData, columns: List[ColumnMetadata], primaryKey: Option[PrimaryKey])

case class Column(metadata: ColumnMetadata, value: Serializable = null) {
  def value[T]: T = {
    value match {
      case null ⇒ null.asInstanceOf[T]
      case v    ⇒ v.asInstanceOf[T]
    }
  }
}

sealed abstract class Mutation[T](val table: Table, val rows: T) {
  def execute()
}

case class InsertMutation(
  override val table: Table,
  override val rows: List[Row])
    extends Mutation[List[Row]](table, rows) {

  def execute() {
    Log.info(s"executing insert mutation")
  }
}

case class UpdateMutation(
  override val table: Table,
  override val rows: List[(Row, Row)])
    extends Mutation[List[(Row, Row)]](table, rows) {

  def execute() {
    Log.info(s"executing update mutation")
  }
}

case class DeleteMutation(
  override val table: Table,
  override val rows: List[Row])
    extends Mutation[List[Row]](table, rows) {

  def execute() {
    Log.info(s"executing delete mutation")
  }
}

class Pipe(id: String, consumers: List[BinlogConsumer], producer: Producer) {

  var CONSUMER_DISCONNECT_WAIT_SECS = 2

  var threads = List.empty[Thread]
  val listener = PipeListener(this)

  def connect() {

    if (threads.size > 0) {

      Log.warning("Attempting to reconnect pipe while already connected, aborting!")

    } else {

      threads = consumers.map(c ⇒ {
        c.registerListener(listener)
        c.registerProducer(id, producer)
        val t = new Thread() {
          override def run() {
            Log.info(s"Connecting pipe between ${c} -> ${producer.getClass}")
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

object PipeListener {
  def apply(pipe: Pipe) = new PipeListener(pipe)
}

class PipeListener(pipe: Pipe) extends Listener {
  def onConnect(consumer: BinlogConsumer) {
    Log.info(s"Pipe $pipe connected!")
  }

  def onDisconnect(consumer: BinlogConsumer) {
    Log.info(s"Pipe $pipe disconnected.")
  }
}
