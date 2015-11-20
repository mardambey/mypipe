package mypipe.producer.stdout

import mypipe.api.event._
import mypipe.api.producer.Producer
import org.slf4j.LoggerFactory
import com.typesafe.config.Config

class StdoutProducer(config: Config) extends Producer(config) {

  protected val mutations = scala.collection.mutable.ListBuffer[String]()
  protected val log = LoggerFactory.getLogger(getClass)

  override def handleAlter(event: AlterEvent): Boolean = {
    log.info(s"\n$event\n")
    true
  }

  override def flush(): Boolean = {
    if (mutations.nonEmpty) {
      log.info("\n" + mutations.mkString("\n"))
      mutations.clear()
    }

    true
  }

  override def queueList(mutationz: List[Mutation]): Boolean = {
    mutationz.foreach(queue)
    true
  }

  override def queue(mutation: Mutation): Boolean = {
    // TODO: quote column values if they are strings before printing
    mutation match {

      case i: InsertMutation ⇒
        mutations += s"INSERT INTO ${i.table.db}.${i.table.name} (${i.table.columns.map(_.name).mkString(", ")}) VALUES ${i.rows.map("(" + _.columns.values.map(_.value).mkString(", ") + ")").mkString(",")}"

      case u: UpdateMutation ⇒
        u.rows.foreach(rr ⇒ {

          val old = rr._1
          val cur = rr._2
          val pKeyColNames = u.table.primaryKey.map(pKey ⇒ pKey.columns.map(_.name))

          val p = pKeyColNames.map(colName ⇒ {
            val cols = old.columns
            cols.filter(_._1.equals(colName))
            cols.head
          })

          val pKeyVals = p.map(_._2.value.toString)
          val where = pKeyColNames
            .map(_.zip(pKeyVals)
              .map(kv ⇒ kv._1 + "=" + kv._2))
            .map(_.mkString(", "))
            .map(w ⇒ s"WHERE ($w)").getOrElse("")

          val curValues = cur.columns.values.map(_.value)
          val colNames = u.table.columns.map(_.name)
          val updates = colNames.zip(curValues).map(kv ⇒ kv._1 + "=" + kv._2).mkString(", ")
          mutations += s"UPDATE ${u.table.db}.${u.table.name} SET ($updates) $where"
        })

      case d: DeleteMutation ⇒
        d.rows.foreach(row ⇒ {

          val pKeyColNames = if (d.table.primaryKey.isDefined) d.table.primaryKey.get.columns.map(_.name) else List.empty[String]

          val p = pKeyColNames.map(colName ⇒ {
            val cols = row.columns
            cols.filter(_._1 == colName)
            cols.head
          })

          val pKeyVals = p.map(_._2.value.toString)
          val where = pKeyColNames.zip(pKeyVals).map(kv ⇒ kv._1 + "=" + kv._2).mkString(", ")
          mutations += s"DELETE FROM ${d.table.db}.${d.table.name} WHERE ($where)"

        })

      case _ ⇒ log.info(s"Ignored mutation: $mutation")

    }

    true
  }

  override def toString: String = {
    "StdoutProducer"
  }

}
