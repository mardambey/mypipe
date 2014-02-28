package mypipe.api

import mypipe.Log
import java.io.Serializable
import com.github.shyiko.mysql.binlog.event.TableMapEventData
import com.github.shyiko.mysql.binlog.event.deserialization.{ ColumnType ⇒ MColumnType }
import mypipe.producer.cassandra.Mapping

abstract class Producer(mappings: List[Mapping]) {
  def queue(mutation: Mutation[_])
  def queueList(mutation: List[Mutation[_]])
  def flush()
}

object ColumnMetadata {
  type ColumnType = MColumnType

  def typeByCode(code: Int): ColumnType = MColumnType.byCode(code)
}

case class ColumnMetadata(name: String, colType: ColumnMetadata.ColumnType)
case class Column(metadata: ColumnMetadata, value: Serializable = null)
case class Row(table: Table, columns: Map[String, Column])
case class Table(id: java.lang.Long, name: String, db: String, evData: TableMapEventData, columns: List[ColumnMetadata])

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

