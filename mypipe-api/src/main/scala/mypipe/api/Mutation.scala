package mypipe.api

sealed abstract class Mutation[T](val table: Table, val rows: T)

abstract class SingleValuedMutation(
  override val table: Table,
  override val rows: List[Row])
    extends Mutation[List[Row]](table, rows)

case class InsertMutation(
  override val table: Table,
  override val rows: List[Row])
    extends SingleValuedMutation(table, rows)

case class UpdateMutation(
  override val table: Table,
  override val rows: List[(Row, Row)])
    extends Mutation[List[(Row, Row)]](table, rows)

case class DeleteMutation(
  override val table: Table,
  override val rows: List[Row])
    extends SingleValuedMutation(table, rows)

trait MutationSerializer[OUTPUT] {
  protected def serialize(input: Mutation[_]): OUTPUT
}

trait MutationDeserializer[INPUT] {
  protected def deserialize(input: INPUT): Mutation[_]
}
