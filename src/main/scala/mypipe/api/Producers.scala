package mypipe.api

import mypipe.Log
import java.io.Serializable
import com.github.shyiko.mysql.binlog.event.TableMapEventData

trait Producer {
  def queue(mutation: Mutation[_])
  def queueList(mutation: List[Mutation[_]])
  def flush()
}

case class Column(name: String)
case class Table(id: java.lang.Long, name: String, db: String, evData: TableMapEventData, columns: List[Column])

abstract class Mutation[T](val table: Table, val rows: T) {
  def execute()
}

case class InsertMutation(
  override val table: Table,
  override val rows: List[Array[Serializable]])
    extends Mutation[List[Array[Serializable]]](table, rows) {

  def execute() {
    Log.info(s"executing insert mutation")
  }
}

case class UpdateMutation(
  override val table: Table,
  override val rows: List[(Array[Serializable], Array[Serializable])])
    extends Mutation[List[(Array[Serializable], Array[Serializable])]](table, rows) {

  def execute() {
    Log.info(s"executing update mutation")
  }
}

case class DeleteMutation(
  override val table: Table,
  override val rows: List[Array[Serializable]])
    extends Mutation[List[Array[Serializable]]](table, rows) {

  def execute() {
    Log.info(s"executing delete mutation")
  }
}

