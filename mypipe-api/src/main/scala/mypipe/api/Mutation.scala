package mypipe.api

sealed abstract class Mutation[T](val table: Table, val rows: T) {
  def execute()
}

case class InsertMutation(
  override val table: Table,
  override val rows: List[Row])
    extends Mutation[List[Row]](table, rows) {

  def execute() {
    Log.info(s"executing insert mutation")
  }
}

case class UpdateMutation(
  override val table: Table,
  override val rows: List[(Row, Row)])
    extends Mutation[List[(Row, Row)]](table, rows) {

  def execute() {
    Log.info(s"executing update mutation")
  }
}

case class DeleteMutation(
  override val table: Table,
  override val rows: List[Row])
    extends Mutation[List[Row]](table, rows) {

  def execute() {
    Log.info(s"executing delete mutation")
  }
}

