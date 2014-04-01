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

/** Serializes a mutation into the given output type.
 *
 *  @tparam Output the type that the Mutation will be serialized to.
 */
trait MutationSerializer[Output] {
  /** Serialize the given mutation into the output type.
   *
   *  @param input mutation to serialize
   *  @return the serialized mutation in Output type
   */
  protected def serialize(input: Mutation[_]): Output
}

trait Serializer[Input, Output] {
  def serialize(topic: String, input: Input): Option[Output]
}

/** Deserializes the data from Input to Output
 *
 *  @tparam Input the input type
 *  @tparam Output the output type
 */
trait Deserializer[Input, Output] {
  /** Deserializes the data from Input to Output
   *  @param topic topic that this data is coming from
   *  @param input the data to deserialize
   *  @return the deserialized data in Output format
   */
  def deserialize(topic: String, input: Input): Option[Output]
}

/** Deserializes data returning Mutation instances.
 *
 *  @tparam Input the type of input data to work with.
 */
trait MutationDeserializer[Input] extends Deserializer[Input, Mutation[_]] {
  /** Deserialize the given input and return a Mutation instance.
   *
   *  @param topic topic that this data is coming from
   *  @param input to deserialize
   *  @return a Mutation (InsertMutation, UpdateMutation, DeleteMutation) instance.
   */
  override def deserialize(topic: String, input: Input): Option[Mutation[_]]
}
