package mypipe.producer

import com.typesafe.config.ConfigFactory
import mypipe.api.event.Mutation
import mypipe.util.Eval

/** Created by peter on 3/3/16.
 */
object TopicUtil {

  val config = ConfigFactory.load()
  val topicFormat = config.getString("mypipe.redis.topic-format")

  val tplFn = Eval[(String, String) ⇒ String]("{ (db: String, table: String) => { s\"" + topicFormat + "\" } }")

  def topic(mutation: Mutation): String =
    topic(mutation.table.db, mutation.table.name)

  def topic(db: String, table: String) =
    tplFn(db, table)
}
