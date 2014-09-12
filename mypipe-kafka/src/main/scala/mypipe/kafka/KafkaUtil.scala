package mypipe.kafka

import mypipe.api.Mutation

object KafkaUtil {

  def genericTopic(mutation: Mutation[_]): String =
    genericTopic(mutation.table.db, mutation.table.name)

  def genericTopic(db: String, table: String) =
    s"${db}_${table}_generic"

  def specificTopic(mutation: Mutation[_]): String =
    specificTopic(mutation.table.db, mutation.table.name)

  def specificTopic(db: String, table: String): String =
    s"${db}_${table}_specific"
}
