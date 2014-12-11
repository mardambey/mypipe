package mypipe.api.event

import mypipe.api.data.Table
import mypipe.api.data.Row

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
   *  @param mutation
   *  @return string representing the mutation's type
   */
  def typeAsString(mutation: Mutation[_]): String = mutation.getClass match {
    case InsertClass ⇒ InsertString
    case UpdateClass ⇒ UpdateString
    case DeleteClass ⇒ DeleteString
    case _           ⇒ UnknownString
  }

  /** Given a mutation returns a magic byte based on it's type.
   *  @param mutation
   *  @return a magic byte
   */
  def getMagicByte(mutation: Mutation[_]): Byte = mutation.getClass match {
    case InsertClass ⇒ InsertByte
    case UpdateClass ⇒ UpdateByte
    case DeleteClass ⇒ DeleteByte
    case _           ⇒ UnknownByte
  }

  /** Given a mutation's magic byte, returns the string
   *  representing this mutation's type.
   *
   *  @param byte
   *  @return string representing the mutation's type
   */
  def byteToString(byte: Byte): String = byte match {
    case Mutation.InsertByte ⇒ Mutation.InsertString
    case Mutation.UpdateByte ⇒ Mutation.UpdateString
    case Mutation.DeleteByte ⇒ Mutation.DeleteString
  }
}
/** Represents a row change event (Insert, Update, or Delete).
 *
 *  @param table that the row belongs to
 *  @param rows changes rows
 *  @tparam T type of the rows
 */
sealed abstract class Mutation[T](val table: Table, val rows: T) extends Event

/** Represents a Mutation that holds a single set of values for each row (Insert or Delete, not Update)
 *
 *  @param table which the mutation affects
 *  @param rows which are changed by the mutation
 */
class SingleValuedMutation(
  override val table: Table,
  override val rows: List[Row])
    extends Mutation[List[Row]](table, rows)

/** Represents an inserted row.
 *
 *  @param table which the mutation affects
 *  @param rows which are changed by the mutation
 */
case class InsertMutation(
  override val table: Table,
  override val rows: List[Row])
    extends SingleValuedMutation(table, rows)

/** Represents an updated row.
 *  @param table that the row belongs to
 *  @param rows changes rows
 */
case class UpdateMutation(
  override val table: Table,
  override val rows: List[(Row, Row)])
    extends Mutation[List[(Row, Row)]](table, rows)

/** Represents a deleted row.
 *
 *  @param table which the mutation affects
 *  @param rows which are changed by the mutation
 */
case class DeleteMutation(
  override val table: Table,
  override val rows: List[Row])
    extends SingleValuedMutation(table, rows)

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

