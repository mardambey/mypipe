package mypipe.api

import com.github.shyiko.mysql.binlog.event.TableMapEventData
import mypipe.util.Enum
import java.io.Serializable

object ColumnType extends Enum {
  sealed trait EnumVal extends Value

  val DECIMAL = new EnumVal { val value = 0 }
  val TINY = new EnumVal { val value = 1 }
  val SHORT = new EnumVal { val value = 2 }
  val LONG = new EnumVal { val value = 3 }
  val FLOAT = new EnumVal { val value = 4 }
  val DOUBLE = new EnumVal { val value = 5 }
  val NULL = new EnumVal { val value = 6 }
  val TIMESTAMP = new EnumVal { val value = 7 }
  val LONGLONG = new EnumVal { val value = 8 }
  val INT24 = new EnumVal { val value = 9 }
  val DATE = new EnumVal { val value = 10 }
  val TIME = new EnumVal { val value = 11 }
  val DATETIME = new EnumVal { val value = 12 }
  val YEAR = new EnumVal { val value = 13 }
  val NEWDATE = new EnumVal { val value = 14 }
  val VARCHAR = new EnumVal { val value = 15 }
  val BIT = new EnumVal { val value = 16 }
  //  = new EnumVal { val value = TIMESTAMP|DATETIME|TIME }_V2 data types appeared in MySQL 5.6.4
  // @see http://dev.mysql.com/doc/internals/en/date-and-time-data-type-representation.html
  val TIMESTAMP_V2 = new EnumVal { val value = 17 }
  val DATETIME_V2 = new EnumVal { val value = 18 }
  val TIME_V2 = new EnumVal { val value = 19 }
  val NEWDECIMAL = new EnumVal { val value = 246 }
  val ENUM = new EnumVal { val value = 247 }
  val SET = new EnumVal { val value = 248 }
  val TINY_BLOB = new EnumVal { val value = 249 }
  val MEDIUM_BLOB = new EnumVal { val value = 250 }
  val LONG_BLOB = new EnumVal { val value = 251 }
  val BLOB = new EnumVal { val value = 252 }
  val VAR_STRING = new EnumVal { val value = 253 }
  val STRING = new EnumVal { val value = 254 }
  val GEOMETRY = new EnumVal { val value = 255 }

  val UNKNOWN = new EnumVal { val value = -9999 }

  def typeByCode(code: Int): Option[ColumnType.EnumVal] = values.find(_.value == code)
}

case class PrimaryKey(columns: List[ColumnMetadata])
case class ColumnMetadata(name: String, colType: ColumnType.EnumVal, isPrimaryKey: Boolean)
case class Row(table: Table, columns: Map[String, Column])
case class Table(id: java.lang.Long, name: String, db: String, columns: List[ColumnMetadata], primaryKey: Option[PrimaryKey])

case class Column(metadata: ColumnMetadata, value: Serializable = null) {
  def value[T]: T = {
    value match {
      case null ⇒ null.asInstanceOf[T]
      case v    ⇒ v.asInstanceOf[T]
    }
  }
}