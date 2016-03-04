package mypipe.api.event

import java.util.UUID

import mypipe.api.data.{ Row, Table }

sealed abstract class Event

sealed trait QueryEvent extends Event {
  val database: String
  val sql: String
}

final case class UnknownEvent(database: String = "", sql: String = "") extends QueryEvent
final case class UnknownQueryEvent(database: String = "", sql: String = "") extends QueryEvent
final case class BeginEvent(database: String, sql: String) extends QueryEvent
final case class CommitEvent(database: String, sql: String) extends QueryEvent
final case class RollbackEvent(database: String, sql: String) extends QueryEvent

sealed trait TableContainingEvent extends QueryEvent {
  val table: Table
}

final case class AlterEvent(table: Table, sql: String) extends TableContainingEvent {
  val database = table.db
}

final case class XidEvent(xid: Long) extends Event
final case class TableMapEvent(tableId: Long, tableName: String, database: String, columnTypes: Array[Byte]) extends Event

/** Represents a row change event (Insert, Update, or Delete).
 *
 *  @param table that the row belongs to
 */
sealed abstract class Mutation(override val table: Table, val txid: UUID, val txQueryCount: Int) extends TableContainingEvent {
  // TODO: populate this field
  val sql = ""
  val database = table.db
  def txAware(txid: UUID): Mutation
  def txQueryCount(txQueryCount: Int): Mutation
}

/** Represents a Mutation that holds a single set of values for each row (Insert or Delete, not Update)
 *
 *  @param table which the mutation affects
 *  @param rows which are changed by the mutation
 */
abstract class SingleValuedMutation(
  override val table: Table,
  val rows: List[Row],
  override val txid: UUID = null,
  override val txQueryCount: Int = 0)
    extends Mutation(table, txid, txQueryCount)

object SingleValuedMutation {
  def primaryKeyAsString(mutation: SingleValuedMutation, row: Row, delim: String = "."): Option[String] = {
    mutation.table.primaryKey.map { pk ⇒
      pk.columns.map { colMetaData ⇒
        row.columns(colMetaData.name)
      }
    } map (_.mkString(delim))
  }
}

/** Represents an inserted row.
 *
 *  @param table which the mutation affects
 *  @param rows which are changed by the mutation
 */
case class InsertMutation(
  override val table: Table,
  override val rows: List[Row],
  override val txid: UUID = null,
  override val txQueryCount: Int = 0)
    extends SingleValuedMutation(table, rows, txid, txQueryCount) {

  override def txAware(txid: UUID = null): Mutation = {
    InsertMutation(table, rows, txid)
  }

  override def txQueryCount(txQueryCount: Int = 0): Mutation = {
    InsertMutation(table, rows, txid, txQueryCount)
  }
}

/** Represents an updated row.
 *  @param table that the row belongs to
 *  @param rows changes rows
 */
case class UpdateMutation(
  override val table: Table,
  rows: List[(Row, Row)],
  override val txid: UUID = null,
  override val txQueryCount: Int = 0)
    extends Mutation(table, txid, txQueryCount) {

  override def txAware(txid: UUID = null): Mutation = {
    UpdateMutation(table, rows, txid)
  }

  override def txQueryCount(txQueryCount: Int = 0): Mutation = {
    UpdateMutation(table, rows, txid, txQueryCount)
  }
}

/** Represents a deleted row.
 *
 *  @param table which the mutation affects
 *  @param rows which are changed by the mutation
 */
case class DeleteMutation(
  override val table: Table,
  override val rows: List[Row],
  override val txid: UUID = null,
  override val txQueryCount: Int = 0)
    extends SingleValuedMutation(table, rows, txid, txQueryCount) {

  override def txAware(txid: UUID = null): Mutation = {
    DeleteMutation(table, rows, txid)
  }

  override def txQueryCount(txQueryCount: Int = 0): Mutation = {
    DeleteMutation(table, rows, txid, txQueryCount)
  }
}

/** General purpose Mutation helpers.
 */
object Mutation {

  val InsertClass = classOf[InsertMutation]
  val UpdateClass = classOf[UpdateMutation]
  val DeleteClass = classOf[DeleteMutation]

  val UnknownByte = 0x0.toByte
  val InsertByte = 0x1.toByte
  val UpdateByte = 0x2.toByte
  val DeleteByte = 0x3.toByte

  val InsertString = "insert"
  val UpdateString = "update"
  val DeleteString = "delete"
  val UnknownString = "unknown"

  /** Given a mutation returns a string representing it's type
   *  @param mutation mutation who's type to stringify
   *  @return string representing the mutation's type
   */
  def typeAsString(mutation: Mutation): String = mutation.getClass match {
    case InsertClass ⇒ InsertString
    case UpdateClass ⇒ UpdateString
    case DeleteClass ⇒ DeleteString
    case _           ⇒ UnknownString
  }

  /** Given a mutation returns a magic byte based on it's type.
   *  @param mutation mutation who's magic byte we want
   *  @return a magic byte
   */
  def getMagicByte(mutation: Mutation): Byte = mutation.getClass match {
    case InsertClass ⇒ InsertByte
    case UpdateClass ⇒ UpdateByte
    case DeleteClass ⇒ DeleteByte
    case _           ⇒ UnknownByte
  }

  /** Given a mutation's magic byte, returns the string
   *  representing this mutation's type.
   *
   *  @param byte magic byte to get a string representation for
   *  @return string representing the mutation's type
   */
  def byteToString(byte: Byte): String = byte match {
    case Mutation.InsertByte ⇒ Mutation.InsertString
    case Mutation.UpdateByte ⇒ Mutation.UpdateString
    case Mutation.DeleteByte ⇒ Mutation.DeleteString
  }
}

trait Serializer[Input, Output] {
  def serialize(topic: String, input: Input): Option[Output]
}

/** Deserializes the data from Input to Output
 *
 *  @tparam Input the input type
 *  @tparam Output the output type
 *  @tparam Schema the schema type
 */
trait Deserializer[Input, Output, Schema] {

  /** Deserializes the data from Input to Output
   *  @param input the data to deserialize
   *  @param offset the offset to deserialize from with the input array
   *  @return the deserialized data in Output format
   */
  def deserialize(schema: Schema, input: Input, offset: Int = 0): Option[Output]
}

