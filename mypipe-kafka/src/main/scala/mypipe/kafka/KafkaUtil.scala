package mypipe.kafka

import com.typesafe.config.ConfigFactory
import mypipe.api.event.Mutation

object KafkaUtil {

  val config = ConfigFactory.load()
  val genericTopicFormat = config.getString("mypipe.kafka.generic-producer.topic-format")
  val specificTopicFormat = config.getString("mypipe.kafka.specific-producer.topic-format")

  def genericTopic(mutation: Mutation[_]): String =
    genericTopic(mutation.table.db, mutation.table.name)

  def genericTopic(db: String, table: String) =
    s"${db}_${table}_generic"

  def specificTopic(mutation: Mutation[_]): String =
    specificTopic(mutation.table.db, mutation.table.name)

  def specificTopic(db: String, table: String): String =
    s"${db}_${table}_specific"
}

