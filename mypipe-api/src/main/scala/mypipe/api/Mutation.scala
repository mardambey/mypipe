package mypipe.api

/**
 * Represents a row change event (Insert, Update, or Delete).
 *
 * @param table that the row belongs to
 * @param rows changes rows
 * @tparam T type of the rows
 */
sealed abstract class Mutation[T](val table: Table, val rows: T)

/**
 * Represents a Mutation that holds a single set of values for each row (Insert or Delete, not Update)
 *
 * @param table which the mutation affects
 * @param rows which are changed by the mutation
 */
class SingleValuedMutation(
  override val table: Table,
  override val rows: List[Row])
    extends Mutation[List[Row]](table, rows)

/**
 * Represents an inserted row.
 *
 * @param table which the mutation affects
 * @param rows which are changed by the mutation
 */
case class InsertMutation(
  override val table: Table,
  override val rows: List[Row])
    extends SingleValuedMutation(table, rows)

/**
 * Represents an updated row.
 * @param table that the row belongs to
 * @param rows changes rows
 */
case class UpdateMutation(
  override val table: Table,
  override val rows: List[(Row, Row)])
    extends Mutation[List[(Row, Row)]](table, rows)

/**
 * Represents a deleted row.
 *
 * @param table which the mutation affects
 * @param rows which are changed by the mutation
 */
case class DeleteMutation(
  override val table: Table,
  override val rows: List[Row])
    extends SingleValuedMutation(table, rows)

/**
 * Serializes a mutation into the given output type.
 *
 * @tparam OUTPUT the type that the Mutation will be serialized to.
 */
trait MutationSerializer[OUTPUT] {
  /**
   * Serialize the given mutation into the output type.
   *
   * @param input mutation to serialize
   * @return the serialized mutation in OUTPUT type
   */
  protected def serialize(input: Mutation[_]): OUTPUT
}

/**
 * Deserializes data returning Mutation instances.
 *
 * @tparam INPUT the type of input data to work with.
 */
trait MutationDeserializer[INPUT] {
  /**
   * Deserialize the given input and return a Mutation instance.
   *
   * @param input to deserialize
   * @return a Mutation (InsertMutation, UpdateMutation, DeleteMutation) instance.
   */
  protected def deserialize[MUTATION](input: INPUT)(implicit m: reflect.ClassTag[MUTATION]): MUTATION
}
