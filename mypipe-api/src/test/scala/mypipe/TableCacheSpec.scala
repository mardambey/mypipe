package mypipe

import java.util.concurrent.{ TimeUnit, LinkedBlockingQueue }

import akka.util.Timeout
import mypipe.api.Table
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

  "TableCache" should "be able to add and get tables to and from the cache" in {

    val consumer = BinlogConsumer(hostname, port.toInt, username, password, BinlogFilePos.current)

    val future = Future[Boolean] {
      val tableCache = new TableCache(db.hostname, db.port, db.username, db.password)
      val queue = new LinkedBlockingQueue[Table](1)

      consumer.registerListener(new BinlogConsumerListener() {
        override def onConnect(c: BinlogConsumer) { connected = true }
        override def onTableMap(c: BinlogConsumer, table: Table) {
          // intercept the table change event
          queue.add(table)
        }
      })

      val f = Future { consumer.connect() }
      while (!connected) Thread.sleep(1)

      // make an insert
      val insertFuture = db.connection.sendQuery(Queries.INSERT.statement(id = "123"))
      Await.result(insertFuture, 2000 millis)

      val table = queue.poll(10, TimeUnit.SECONDS)
      tableCache.addTableByEvent(Long.unbox(table.id), table.name, table.db, table.columns.map(_.colType.value.toByte).toArray)
      val table2 = tableCache.getTable(table.id)
      assert(table2.isDefined)
      assert(table2.get.name == Queries.TABLE.name)
      assert(table2.get.db == table.db)
      assert(table2.get.columns == table.columns)
      true
    }

    try {
      val ret = Await.result(future, 10 seconds)
      consumer.disconnect()
      assert(ret)
    } catch {
      case e: Exception ⇒ {
        consumer.disconnect()
        log.error(s"Caught exception: ${e.getMessage} at ${e.getStackTraceString}")
        assert(false)
      }
    }
  }
}
