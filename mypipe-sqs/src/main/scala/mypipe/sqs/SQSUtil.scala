package mypipe.sqs

import com.typesafe.config.ConfigFactory
import mypipe.api.event.Mutation
import mypipe.util.Eval

object SQSUtil {

  val config = ConfigFactory.load()
  val topicFormat = config.getString("mypipe.sqs.topic-format")

  val tplFn = Eval[(String, String) ⇒ String]("{ (db: String, table: String) => { s\"" + topicFormat + "\" } }")

  def topic(mutation: Mutation): String =
    topic(mutation.table.db, mutation.table.name)

  def topic(db: String, table: String) =
    tplFn(db, table)
}
