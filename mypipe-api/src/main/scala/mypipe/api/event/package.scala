package mypipe.api

package object event {

  trait Event

  trait QueryEvent extends Event {
    val database: String
    val sql: String
  }

  case class UnknownEvent(database: String = "", sql: String = "") extends QueryEvent
  case class BeginEvent(database: String, sql: String) extends QueryEvent
  case class CommitEvent(database: String, sql: String) extends QueryEvent
  case class RollbackEvent(database: String, sql: String) extends QueryEvent
  case class AlterEvent(database: String, sql: String) extends QueryEvent
  case class XidEvent(xid: Long) extends Event
  case class TableMapEvent(tableId: Long, tableName: String, database: String, columnTypes: Array[Byte]) extends Event
}
