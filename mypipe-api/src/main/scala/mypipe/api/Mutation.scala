package mypipe.api

/** Represents a row change event (Insert, Update, or Delete).
 *
 *  @param table that the row belongs to
 *  @param rows changes rows
 *  @tparam T type of the rows
 */
sealed abstract class Mutation[T](val table: Table, val rows: T)

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
 *  @tparam SchemaId the schema ID type
 */
trait Deserializer[Input, Output, SchemaId] {
  /** Deserializes the data from Input to Output
   *  @param topic topic that this data is coming from
   *  @param input the data to deserialize
   *  @param offset the offset to deserialize from with the input array
   *  @return the deserialized data in Output format
   */
  def deserialize(topic: String, schemaId: SchemaId, input: Input, offset: Int = 0): Option[Output]
}

