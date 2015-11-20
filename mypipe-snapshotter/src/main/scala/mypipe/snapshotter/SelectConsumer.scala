package mypipe.snapshotter

import scala.concurrent.duration._
import scala.concurrent.{ Future, Await }
import akka.actor._
import akka.pattern.ask
import akka.util.Timeout

import mypipe.api.consumer.BinaryLogConsumer
import mypipe.api.data._
import mypipe.api.event._
import mypipe.mysql._

import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.collection.immutable.ListMap

case class SelectEvent(database: String, table: String, rows: Seq[Seq[Any]])

class SelectConsumer(
  override protected val username: String,
  override protected val hostname: String,
  override protected val password: String,
  override protected val port: Int)
    extends BinaryLogConsumer[SelectEvent, Unit]
    with ConfigBasedErrorHandlingBehaviour[SelectEvent, Unit]
    with ConfigBasedEventSkippingBehaviour
    with CacheableTableMapBehaviour {

  private val log = LoggerFactory.getLogger(getClass)
  private val system = ActorSystem("mypipe-snapshotter")
  private val dbMetadata = system.actorOf(MySQLMetadataManager.props(hostname, port, username, Some(password)), s"SelectConsumer-DBMetadataActor-$hostname:$port")
  private implicit val ec = system.dispatcher
  private implicit val timeout = Timeout(2.second)
  private val tables = scala.collection.mutable.HashMap[String, Table]()

  def handleEvents(selects: Seq[Option[SelectEvent]]) = {
    log.info(s"handling seelcts, size=${selects.length}")
    selects.foreach {
      case Some(select) ⇒
        log.info(s"handling select $select")
        decodeEvent(select).foreach(s ⇒ listeners.foreach(_.onMutation(this, s.asInstanceOf[Mutation])))
      case None ⇒
    }
  }

  /** Given a third-party BinLogEvent, this method decodes it to an
   *  mypipe specific Event type if it recognizes it.
   *  @param event the event to decode
   *  @return the decoded Event or None
   */
  override protected def decodeEvent(event: SelectEvent): Option[Event] = {
    val rowData = event.rows.map(_.map(_.asInstanceOf[java.io.Serializable]).toArray).toList.asJava
    getTable(event.database, event.table) match {
      case Some(table) ⇒
        val rows = createRows(table, rowData)
        log.info(s"Got back rows: $rows")
        Some(InsertMutation(table, rows))
      case None ⇒
        log.error(s"Could not find table for event, skipping: $event")
        None
    }
  }

  protected def createRows(table: Table, evRows: java.util.List[Array[java.io.Serializable]]): List[Row] = {
    evRows.asScala.map(evRow ⇒ {

      // zip the names and values from the table's columns and the row's data and
      // create a map that contains column names to Column objects with values
      val cols = table.columns.zip(evRow).map(c ⇒ c._1.name -> Column(c._1, c._2))
      val columns = ListMap.empty[String, Column] ++ cols.toArray

      Row(table, columns)

    }).toList
  }

  /** Gets the consumer's current position in the binary log.
   *  @return current BinLogPos
   */
  override def getBinaryLogPosition: Option[Unit] = None

  /** Gets this consumer's unique ID.
   *  @return Unique ID as a string.
   */
  override def id: String = s"select-consumer-$hostname-$port"

  override protected def onStop(): Unit = Unit
  override protected def onStart(): Unit = Unit

  private def getTable(database: String, table: String): Option[Table] = {
    tables.get(s"$database.$table") match {
      case table @ Some(_) ⇒ table
      case None ⇒
        val future = ask(dbMetadata, GetColumns(database, table)).asInstanceOf[Future[(List[ColumnMetadata], Option[PrimaryKey])]]
        try {
          val columns = Await.result(future, 2.seconds)
          val t = Table(0L, table, database, columns._1, columns._2)
          tables.put(s"$database.$table", t)
          Some(t)
        } catch {
          case e: Exception ⇒
            log.error(s"Exception caught while fetching table information for $database.$table")
            None
        }
    }
  }
}
