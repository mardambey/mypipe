package mypipe.producer.stdout

import mypipe.api._
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
        mutations += s"INSERT INTO ${i.table.db}.${i.table.name} (${i.table.columns.map(_.name).mkString(", ")}) VALUES (${i.rows.head.columns.values.map(_.value).mkString(", ")})"
      }

      case u: UpdateMutation ⇒ {
        u.rows.foreach(rr ⇒ {

          val old = rr._1
          val cur = rr._2
          val pKeyColNames = if (u.table.primaryKey.isDefined) u.table.primaryKey.get.columns.map(_.name) else List.empty[String]

          val p = pKeyColNames.map(colName ⇒ {
            val cols = old.columns
            cols.filter(_._1.equals(colName))
            cols.head
          })

          val pKeyVals = p.map(_._2.value.toString)
          mutations += s"UPDATE ${u.table.db}.${u.table.name} SET ${u.table.columns.map(_.name).zip(cur.columns.values.map(_.value)).map(kv ⇒ kv._1 + "=" + kv._2).mkString(", ")} WHERE (${pKeyColNames.zip(pKeyVals).map(kv ⇒ kv._1 + "=" + kv._2).mkString(", ")})"

        })

      }

      case d: DeleteMutation ⇒ {
      }

    }
  }

}
