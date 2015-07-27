package mypipe

import java.util.concurrent.{ TimeUnit, LinkedBlockingQueue }
import com.github.shyiko.mysql.binlog.event.{ Event ⇒ MEvent }

import akka.util.Timeout
import mypipe.api.consumer.{ BinaryLogConsumer, BinaryLogConsumerListener }
import mypipe.api.data.Table
import mypipe.api.event.{ AlterEvent, TableMapEvent }
import mypipe.mysql._
import org.scalatest.BeforeAndAfterAll
import org.slf4j.LoggerFactory

import scala.concurrent.duration._
import scala.concurrent.{ TimeoutException, Await, Future }

class TableCacheSpec extends UnitSpec with DatabaseSpec with ActorSystemSpec with BeforeAndAfterAll {

  val log = LoggerFactory.getLogger(getClass)

  implicit val timeout = Timeout(1.second)

  var consumer: MySQLBinaryLogConsumer = _
  var tableCache: TableCache = _
  val queueTableMap = new LinkedBlockingQueue[Table](1)
  val queueTableAlter = new LinkedBlockingQueue[Table](1)

  override def beforeAll() = {
    super.beforeAll()

    @volatile var connected = false
    consumer = MySQLBinaryLogConsumer(Queries.DATABASE.host, Queries.DATABASE.port, Queries.DATABASE.username, Queries.DATABASE.password)
    tableCache = new TableCache(db.hostname, db.port, db.username, db.password)

    consumer.registerListener(new BinaryLogConsumerListener[MEvent, BinaryLogFilePosition]() {
      override def onConnect(c: BinaryLogConsumer[MEvent, BinaryLogFilePosition]): Unit = connected = true
      override def onTableMap(c: BinaryLogConsumer[MEvent, BinaryLogFilePosition], table: Table): Boolean = {
        queueTableMap.add(table)
      }
      override def onTableAlter(c: BinaryLogConsumer[MEvent, BinaryLogFilePosition], event: AlterEvent): Boolean = {
        val table = tableCache.refreshTable(event.database, event.table.name)
        queueTableAlter.add(table.get)
        true
      }
    })

    Future { consumer.connect() }
    while (!connected) Thread.sleep(1)
  }

  override def afterAll(): Unit = {
    consumer.disconnect()
    super.afterAll()
  }

  "TableCache" should "be able to add and get tables to and from the cache" in {

    val future = Future[Boolean] {

      // make an insert
      val insertFuture = db.connection.sendQuery(Queries.INSERT.statement(id = "123"))
      Await.result(insertFuture, 2000.millis)

      val table = queueTableMap.poll(10, TimeUnit.SECONDS)
      tableCache.addTableByEvent(TableMapEvent(Long.unbox(table.id), table.name, table.db, table.columns.map(_.colType.value.toByte).toArray))
      val table2 = tableCache.getTable(table.id)
      assert(table2.isDefined)
      assert(table2.get.name == Queries.TABLE.name)
      assert(table2.get.db == table.db)
      assert(table2.get.columns == table.columns)
      true
    }

    try {
      val ret = Await.result(future, 10.seconds)
      assert(ret)
    } catch {
      case e: Exception ⇒
        log.error(s"Caught exception: ${e.getMessage} at ${e.getStackTraceString}")
        assert(false)
    }
  }

  it should "be able to refresh metadata" in {

    val future = Future[Boolean] {

      val insertFuture = db.connection.sendQuery(Queries.INSERT.statement(id = "124"))
      Await.result(insertFuture, 5.seconds)

      val alterAddFuture = db.connection.sendQuery(Queries.ALTER.statementAdd)
      Await.result(alterAddFuture, 5.seconds)

      val table = queueTableAlter.poll(10, TimeUnit.SECONDS)
      assert(table.columns.exists(_.name == "email"))

      val alterDropFuture = db.connection.sendQuery(Queries.ALTER.statementDrop)
      Await.result(alterDropFuture, 5.seconds)

      val table2 = queueTableAlter.poll(10, TimeUnit.SECONDS)
      assert(!table2.columns.exists(_.name == "email"))

      true
    }

    try {
      val ret = Await.result(future, 35.seconds)
      assert(ret)
    } catch {
      case e: TimeoutException ⇒
        log.error(s"Caught exception: ${e.getMessage} at ${e.getStackTraceString}")
        assert(false)
    }
  }
}
