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

  protected val log = LoggerFactory.getLogger(getClass)
  protected val system = ActorSystem("mypipe-snapshotter")
  protected val dbMetadata = system.actorOf(MySQLMetadataManager.props(hostname, port, username, Some(password)), s"DBMetadataActor-$hostname:$port")
  protected implicit val ec = system.dispatcher
  protected implicit val timeout = Timeout(2.second)

  def handleEvents(selects: Seq[Option[SelectEvent]]) = {
    selects.foreach {
      case Some(select) ⇒
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
    val future = ask(dbMetadata, GetColumns(event.database, event.table)).asInstanceOf[Future[(List[ColumnMetadata], Option[PrimaryKey])]]
    val columns = Await.result(future, 2.seconds)
    val table = Table(0L, event.table, event.database, columns._1, columns._2)
    val rowData = event.rows.map(_.map(_.asInstanceOf[java.io.Serializable]).toArray).toList.asJava
    val rows = createRows(table, rowData)
    Some(InsertMutation(table, rows))
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
}
