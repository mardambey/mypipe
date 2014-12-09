package mypipe.api.data

import mypipe.util.Enum

object ColumnType extends Enum {
  sealed trait EnumVal extends Value {
    val str: String // All ColumnType values also contain a string form
  }

  val DECIMAL = new EnumVal { val value = 0; val str = "decimal" }
  val TINY = new EnumVal { val value = 1; val str = "tinyint" }
  val SHORT = new EnumVal { val value = 2; val str = "smallint" }
  val LONG = new EnumVal { val value = 3; val str = "bigint" }
  val FLOAT = new EnumVal { val value = 4; val str = "float" }
  val DOUBLE = new EnumVal { val value = 5; val str = "double" }
  val NULL = new EnumVal { val value = 6; val str = "null" }
  val TIMESTAMP = new EnumVal { val value = 7; val str = "timestamp" }
  val LONGLONG = new EnumVal { val value = 8; val str = "bigint" }
  val INT24 = new EnumVal { val value = 9; val str = "int" }
  val DATE = new EnumVal { val value = 10; val str = "date" }
  val TIME = new EnumVal { val value = 11; val str = "time" }
  val DATETIME = new EnumVal { val value = 12; val str = "datetime" }
  val YEAR = new EnumVal { val value = 13; val str = "year" }
  val NEWDATE = new EnumVal { val value = 14; val str = "date" }
  val VARCHAR = new EnumVal { val value = 15; val str = "varchar" }
  val BIT = new EnumVal { val value = 16; val str = "bit" }
  // { TIMESTAMP|DATETIME|TIME }_V2 data types appeared in MySQL 5.6.4
  // @see http://dev.mysql.com/doc/internals/en/date-and-time-data-type-representation.html
  val TIMESTAMP_V2 = new EnumVal { val value = 17; val str = "timestamp" }
  val DATETIME_V2 = new EnumVal { val value = 18; val str = "datetime" }
  val TIME_V2 = new EnumVal { val value = 19; val str = "time" }
  val NEWDECIMAL = new EnumVal { val value = 246; val str = "decimal" }
  val ENUM = new EnumVal { val value = 247; val str = "enum" }
  val SET = new EnumVal { val value = 248; val str = "set" }
  val TINY_BLOB = new EnumVal { val value = 249; val str = "tinyblob" }
  val MEDIUM_BLOB = new EnumVal { val value = 250; val str = "mediumblob" }
  val LONG_BLOB = new EnumVal { val value = 251; val str = "longblob" }
  val BLOB = new EnumVal { val value = 252; val str = "blob" }
  val VAR_STRING = new EnumVal { val value = 253; val str = "text" }
  val STRING = new EnumVal { val value = 254; val str = "text" }
  val GEOMETRY = new EnumVal { val value = 255; val str = "geometry" }

  val UNKNOWN = new EnumVal { val value = -9999; val str = "unknown" }

  def typeByCode(code: Int): Option[ColumnType.EnumVal] = values.find(_.value == code)
  def typeByString(str: String): Option[ColumnType.EnumVal] = values.find(_.str == str)
}

