package mypipe.api.data

import java.lang.{Long ⇒ JLong}

case class PrimaryKey(columns: List[ColumnMetadata])

case class ColumnMetadata(name: String, colType: ColumnType.EnumVal, isPrimaryKey: Boolean)

case class Row(table: Table, columns: Map[String, Column])

case class Table(id: JLong, name: String, db: String, columns: List[ColumnMetadata], primaryKey: Option[PrimaryKey])
class UnknownTable(override val id: JLong, override val name: String, override val db: String) extends Table(id, name, db, columns = List.empty, primaryKey = None)

case class Column(metadata: ColumnMetadata, value: java.io.Serializable = null) {

  def value[T]: T = {
    value match {
      case null ⇒ null.asInstanceOf[T]
      case v    ⇒ v.asInstanceOf[T]
    }
  }

  def valueOption[T]: Option[T] = Option(value[T])
}
