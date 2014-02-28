package mypipe.producer.stdout

import mypipe.api._
import mypipe.producer.cassandra.Mapping
import mypipe.Log
import mypipe.api.UpdateMutation
import mypipe.api.InsertMutation

class StdoutProducer(mappings: List[Mapping]) extends Producer(mappings) {

  val mutations = scala.collection.mutable.ListBuffer[String]()

  override def flush() {
    if (mutations.size > 0) {
      Log.info("\n" + mutations.mkString("\n"))
      mutations.clear()
    }
  }

  override def queueList(mutationz: List[Mutation[_]]) {
    mutationz.foreach(queue)
  }

  override def queue(mutation: Mutation[_]) {
    mutation match {

      case i: InsertMutation ⇒ {
        mutations += s"INSERT INTO ${i.table.db}.${i.table.name} (${i.table.columns.map(_.name).mkString(", ")}}) VALUES (${i.rows.head.columns.values.map(_.value).mkString(", ")}})"
      }

      case u: UpdateMutation ⇒ {
        mutations += s"UPDATE ${u.table.db}.${u.table.name} SET ${u.table.columns.map(_.name).zip(u.rows.head._2.columns.values.map(_.value)).map(kv ⇒ kv._1 + "=" + kv._2).mkString(", ")} WHERE /*TODO*/"
      }

      case d: DeleteMutation ⇒ {
      }

    }
  }

}
