package mypipe.kafka

import mypipe.api.Mutation

object KafkaUtil {

  def genericTopic(mutation: Mutation[_]): String = {
    s"${mutation.table.db}_${mutation.table.name}_generic"
  }

  def specificTopic(mutation: Mutation[_]): String = {
    s"${mutation.table.db}_${mutation.table.name}_${Mutation.typeAsString(mutation)}_specific"
  }
}
