package mypipe.producer

import mypipe.Log
import java.io.Serializable

trait Producer {
  def queue(mutation: Mutation[_])
  def queueList(mutation: List[Mutation[_]])
  def flush()
}

abstract class Mutation[T](val db: String, val table: String, val rows: T) {
  def execute()
}

case class InsertMutation(
  override val db: String,
  override val table: String,
  override val rows: List[Array[Serializable]])
    extends Mutation[List[Array[Serializable]]](db, table, rows) {

  def execute() {
    Log.info(s"executing insert mutation")
  }
}

case class UpdateMutation(
  override val db: String,
  override val table: String,
  override val rows: List[(Array[Serializable], Array[Serializable])])
    extends Mutation[List[(Array[Serializable], Array[Serializable])]](db, table, rows) {

  def execute() {
    Log.info(s"executing update mutation")
  }
}

case class DeleteMutation(
  override val db: String,
  override val table: String,
  override val rows: List[Array[Serializable]])
    extends Mutation[List[Array[Serializable]]](db, table, rows) {

  def execute() {
    Log.info(s"executing delete mutation")
  }
}

