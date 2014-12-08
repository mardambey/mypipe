package mypipe.api

trait QueryEvent {
  val database: String
  val sql: String
}

case class BeginEvent(database: String, sql: String) extends QueryEvent
case class CommitEvent(database: String, sql: String) extends QueryEvent
case class RollbackEvent(database: String, sql: String) extends QueryEvent
case class AlterEvent(database: String, sql: String) extends QueryEvent
case class XidEvent(xid: Long)

