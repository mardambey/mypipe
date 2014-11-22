package mypipe

import java.util.concurrent.{ TimeUnit, LinkedBlockingQueue }

import akka.util.Timeout
import mypipe.api.{ TableMapEvent, Table }
import mypipe.mysql._
import org.scalatest.BeforeAndAfterAll
import org.slf4j.LoggerFactory

import scala.concurrent.duration._
import scala.concurrent.{ Await, Future }

class TableCacheSpec extends UnitSpec with DatabaseSpec with ActorSystemSpec with BeforeAndAfterAll {

  @volatile var connected = false
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

  val queue = new LinkedBlockingQueue[Table](1)

  val consumer = BinaryLogConsumer(Queries.DATABASE.host, Queries.DATABASE.port, Queries.DATABASE.username, Queries.DATABASE.password, BinaryLogFilePosition.current)
  consumer.registerListener(new BinaryLogConsumerListener() {
    override def onConnect(c: BinaryLogConsumerTrait): Unit = connected = true
    override def onTableMap(c: BinaryLogConsumerTrait, table: Table): Unit = queue.add(table)
    override def onTableAlter(c: BinaryLogConsumerTrait, table: Table): Unit = queue.add(table)
  })

  val tableCache = new TableCache(db.hostname, db.port, db.username, db.password)

  "TableCache" should "be able to add and get tables to and from the cache" in {

    val future = Future[Boolean] {

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
  }

  it should "be able to refresh metadata" in {

    db.connection.sendQuery(Queries.ALTER.statementAdd)
    val table = queue.poll(10, TimeUnit.SECONDS)
    assert(table.columns.find(column => column.name == "email").isDefined)

    db.connection.sendQuery(Queries.ALTER.statementDrop)
    val table = queue.poll(10, TimeUnit.SECONDS)
    assert(table.columns.find(column => column.name == "email").isEmpty)
  }

  consumer.disconnect()
}
