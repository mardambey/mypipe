package mypipe

import java.util.concurrent.{ TimeUnit, LinkedBlockingQueue }

import akka.util.Timeout
import mypipe.api.consumer.{ BinaryLogConsumerListener, AbstractBinaryLogConsumer }
import mypipe.api.data.Table
import mypipe.api.event.TableMapEvent
import mypipe.mysql._
import org.scalatest.BeforeAndAfterAll
import org.slf4j.LoggerFactory

import scala.concurrent.duration._
import scala.concurrent.{ Await, Future }

class TableCacheSpec extends UnitSpec with DatabaseSpec with ActorSystemSpec with BeforeAndAfterAll {

  val log = LoggerFactory.getLogger(getClass)

  override def beforeAll() {
    db.connect
    Await.result(db.connection.sendQuery(Queries.TRUNCATE.statement), 1 second)
  }

  override def afterAll() {
    try {
      db.disconnect
    } catch { case t: Throwable ⇒ }
  }

  implicit val timeout = Timeout(1 second)

  "TableCache" should "be able to add and get tables to and from the cache" in {

    @volatile var connected = false
    val consumer = MySQLBinaryLogConsumer(Queries.DATABASE.host, Queries.DATABASE.port, Queries.DATABASE.username, Queries.DATABASE.password, BinaryLogFilePosition.current)
    val tableCache = new TableCache(db.hostname, db.port, db.username, db.password)

    val future = Future[Boolean] {

      val queue = new LinkedBlockingQueue[Table](1)
      consumer.registerListener(new BinaryLogConsumerListener() {
        override def onConnect(c: AbstractBinaryLogConsumer): Unit = connected = true
        override def onTableMap(c: AbstractBinaryLogConsumer, table: Table): Unit = queue.add(table)
      })

      Future { consumer.connect() }
      while (!connected) Thread.sleep(1)

      // make an insert
      val insertFuture = db.connection.sendQuery(Queries.INSERT.statement(id = "123"))
      Await.result(insertFuture, 2000 millis)

      val table = queue.poll(10, TimeUnit.SECONDS)
      tableCache.addTableByEvent(TableMapEvent(Long.unbox(table.id), table.name, table.db, table.columns.map(_.colType.value.toByte).toArray))
      val table2 = tableCache.getTable(table.id)
      assert(table2.isDefined)
      assert(table2.get.name == Queries.TABLE.name)
      assert(table2.get.db == table.db)
      assert(table2.get.columns == table.columns)
      true
    }

    try {
      val ret = Await.result(future, 10 seconds)
      assert(ret)
    } catch {
      case e: Exception ⇒ {
        log.error(s"Caught exception: ${e.getMessage} at ${e.getStackTraceString}")
        assert(false)
      }
    }

    consumer.disconnect
  }

  it should "be able to refresh metadata" in {

    @volatile var connected = false
    val consumer = MySQLBinaryLogConsumer(Queries.DATABASE.host, Queries.DATABASE.port, Queries.DATABASE.username, Queries.DATABASE.password, BinaryLogFilePosition.current)

    val future = Future[Boolean] {

      val queue = new LinkedBlockingQueue[Table](1)
      consumer.registerListener(new BinaryLogConsumerListener() {
        override def onConnect(c: AbstractBinaryLogConsumer): Unit = connected = true
        override def onTableAlter(c: AbstractBinaryLogConsumer, table: Table): Unit = {
          queue.add(table)
        }
      })

      Future { consumer.connect() }
      while (!connected) Thread.sleep(1)

      val insertFuture = db.connection.sendQuery(Queries.INSERT.statement(id = "124"))
      Await.result(insertFuture, 5 seconds)

      val alterAddFuture = db.connection.sendQuery(Queries.ALTER.statementAdd)
      Await.result(alterAddFuture, 5 seconds)

      val table = queue.poll(10, TimeUnit.SECONDS)
      assert(table.columns.find(column ⇒ column.name == "email").isDefined)

      val alterDropFuture = db.connection.sendQuery(Queries.ALTER.statementDrop)
      Await.result(alterDropFuture, 5 seconds)

      val table2 = queue.poll(10, TimeUnit.SECONDS)
      assert(table2.columns.find(column ⇒ column.name == "email").isEmpty)

      true
    }

    try {
      val ret = Await.result(future, 35 seconds)
      assert(ret)
    } catch {
      case e: Exception ⇒ {
        log.error(s"Caught exception: ${e.getMessage} at ${e.getStackTraceString}")
        assert(false)
      }
    } finally {
      consumer.disconnect()
    }
  }
}
