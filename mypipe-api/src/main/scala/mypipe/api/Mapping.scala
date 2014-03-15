package mypipe.api

abstract class Mapping {
  def map(mutation: InsertMutation) {}
  def map(mutation: UpdateMutation) {}
  def map(mutation: DeleteMutation) {}
}