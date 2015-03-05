package mypipe.kafka

import com.typesafe.config.ConfigFactory
import mypipe.api.event.Mutation
import mypipe.util.Eval

object KafkaUtil {

  val config = ConfigFactory.load()
  val genericTopicFormat = config.getString("mypipe.kafka.generic-producer.topic-format")
  val specificTopicFormat = config.getString("mypipe.kafka.specific-producer.topic-format")

  val generictplFn = Eval[(String, String) ⇒ String]("{ (db: String, table: String) => { s\"" + genericTopicFormat + "\" } }")
  val specifictplFn = Eval[(String, String) ⇒ String]("{ (db: String, table: String) => { s\"" + specificTopicFormat + "\" } }")

  def genericTopic(mutation: Mutation): String =
    genericTopic(mutation.table.db, mutation.table.name)

  def genericTopic(db: String, table: String) =
    generictplFn(db, table)

  def specificTopic(mutation: Mutation): String =
    specificTopic(mutation.table.db, mutation.table.name)

  def specificTopic(db: String, table: String): String =
    specifictplFn(db, table)
}

