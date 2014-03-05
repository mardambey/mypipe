package mypipe.sample.mappings

import mypipe.producer.cassandra.CassandraMapping
import mypipe.producer.cassandra.CassandraMapping._
import mypipe.api.DeleteMutation
import mypipe.api.UpdateMutation
import mypipe.api.InsertMutation

import com.netflix.astyanax.util.TimeUUIDUtils

class CassandraProfileMapping extends CassandraMapping {

  // create keyspace logs;
  // create column family profile_counters with default_validation_class=CounterColumnType;
  // create column family wswl with comparator=TimeUUIDType;

  val wswl = columnFamily(
    name = "wswl",
    keySer = STRING,
    colSer = TIMEUUID)

  val counters = columnFamily(
    name = "profile_counters",
    keySer = STRING,
    colSer = STRING)

  override def map(mutation: UpdateMutation) = None
  override def map(mutation: DeleteMutation) = None

  override def map(i: InsertMutation) {

    (i.table.db, i.table.name) match {

      case ("logging", "WhosSeenWhoLog") ⇒ {

        val row = i.rows.head.columns
        val rowKey = row("profile_id").value.toString
        val time = row("log_time").value.asInstanceOf[Int].toLong
        val timeUUID = TimeUUIDUtils.getTimeUUID(time)

        val m = mutation("logs")

        m.withRow(wswl, rowKey)
          .putColumn(timeUUID, row("listed_profile_id").value.toString)

        m.withRow(counters, rowKey)
          .incrementCounterColumn("views", 1)
      }

      case ("logging", "foo") ⇒ {
      }

      case x ⇒ {
      }
    }
  }
}

