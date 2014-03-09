package mappings

import mypipe.producer.cassandra.CassandraMapping
import mypipe.producer.cassandra.CassandraMapping._
import mypipe.api.DeleteMutation
import mypipe.api.UpdateMutation
import mypipe.api.InsertMutation
import java.lang.Long

import com.netflix.astyanax.util.TimeUUIDUtils

class CassandraProfileMapping extends CassandraMapping {

  // create keyspace logs;
  // create column family profile_counters with default_validation_class=CounterColumnType;
  // create column family wswl with comparator=TimeUUIDType;
  // create column family pad with comparator = UTF8Type and column_metadata = [
  // { column_name : 'campaign_id', validation_class : Int32Type },
  // { column_name : 'link_id', validation_class : Int32Type },
  // { column_name : 'ad_id', validation_class : Int32Type },
  // { column_name : 'referer', validation_class : UTF8Type },
  // { column_name : 'query', validation_class : UTF8Type },
  // { column_name : 'x_forwarded_for', validation_class : UTF8Type },
  // { column_name : 'affiliate_id', validation_class : Int32Type },
  // { column_name : 'user_agent', validation_class : UTF8Type },
  // { column_name : 'credited_date', validation_class : UTF8Type },
  // { column_name : 'ip_address', validation_class : UTF8Type },
  // { column_name : 'country', validation_class : Int32Type },
  // { column_name : 'region_code', validation_class : UTF8Type },
  // { column_name : 'city_name', validation_class : UTF8Type }];

  val wswl = columnFamily(
    name = "wswl",
    keySer = LONG,
    colSer = TIMEUUID)

  val counters = columnFamily(
    name = "profile_counters",
    keySer = LONG,
    colSer = STRING)

  val pad = columnFamily(
    name = "pad",
    keySer = INT,
    colSer = STRING)

  override def map(mutation: UpdateMutation) = None
  override def map(mutation: DeleteMutation) = None

  override def map(i: InsertMutation) {

    (i.table.db, i.table.name) match {

      case ("logging", "WhosSeenWhoLog") ⇒ {

        val row = i.rows.head.columns
        val rowKey = row("profile_id").value[Long]
        val time = row("log_time").value[Int].toLong
        val timeUUID = TimeUUIDUtils.getTimeUUID(time)

        val m = mutation("logs")

        m.withRow(wswl, rowKey)
          .putColumn(timeUUID, row("listed_profile_id").value[Long])

        m.withRow(counters, rowKey)
          .incrementCounterColumn("views", 1)
      }

      case ("general", "ProfileAdData") ⇒ {

        val row = i.rows.head.columns
        val rowKey = row("profile_id").value[Integer]

        val m = mutation("logs")

        m.withRow(pad, rowKey)
          .putColumn("campaign_id", row("campaign_id").value[Int])
          .putColumn("link_id", row("link_id").value[Int])
          .putColumn("ad_id", row("ad_id").value[Int])
          .putColumn("referer", row("referer").value[String])
          .putColumn("query", row("query").value[String])
          .putColumn("x_forwarded_for", row("x_forwarded_for").value[String])
          .putColumn("affiliate_id", row("affiliate_id").value[Int])
          .putColumn("user_agent", row("user_agent").value[String])
          .putColumn("credited_date", row("credited_date").value[String])
          .putColumn("ip_address", row("ip_address").value[String])
          .putColumn("country", row("country").value[Int])
          .putColumn("region_code", row("region_code").value[String])
          .putColumn("city_name", row("city_name").value[String])
      }

      case x ⇒ {
      }
    }

  }
}

