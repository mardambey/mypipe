package mypipe

import mypipe.api.event.{ DeleteMutation, Mutation, UpdateMutation, InsertMutation }
import mypipe.mysql.{ BinaryLogFilePosition, MySQLBinaryLogConsumer }
import mypipe.pipe.Pipe
import scala.concurrent.Await
import scala.concurrent.duration._
import mypipe.producer.QueueProducer
import java.util.concurrent.{ TimeUnit, LinkedBlockingQueue }
import org.scalatest.BeforeAndAfterAll
import org.slf4j.LoggerFactory

class MySQLSpec extends UnitSpec with DatabaseSpec with ActorSystemSpec with BeforeAndAfterAll {

  val log = LoggerFactory.getLogger(getClass)
  @volatile var connected = false

  val queue = new LinkedBlockingQueue[Mutation[_]]()
  val queueProducer = new QueueProducer(queue)
  val consumer = MySQLBinaryLogConsumer(Queries.DATABASE.host, Queries.DATABASE.port, Queries.DATABASE.username, Queries.DATABASE.password, BinaryLogFilePosition.current)
  val pipe = new Pipe("test-pipe", List(consumer), queueProducer)

  override def beforeAll() {

    db.connect()
    pipe.connect()

    while (!db.connection.isConnected || !pipe.isConnected) { Thread.sleep(1) }

    Await.result(db.connection.sendQuery(Queries.CREATE.statement), 1.second)
    Await.result(db.connection.sendQuery(Queries.TRUNCATE.statement), 1.second)
  }

  override def afterAll() {
    pipe.disconnect()
    db.disconnect()
  }

  "A binlog consumer" should "properly consume insert events" in withDatabase { db ⇒

    db.connection.sendQuery(Queries.INSERT.statement)

    log.info("Waiting for binary log event to arrive.")
    val mutation = queue.poll(30, TimeUnit.SECONDS)

    // expect the row back
    assert(mutation != null)
    assert(mutation.isInstanceOf[InsertMutation])
  }

  "A binlog consumer" should "properly consume update events" in withDatabase { db ⇒

    db.connection.sendQuery(Queries.UPDATE.statement)

    log.info("Waiting for binary log event to arrive.")
    val mutation = queue.poll(30, TimeUnit.SECONDS)

    // expect the row back
    assert(mutation != null)
    assert(mutation.isInstanceOf[UpdateMutation])
  }

  "A binlog consumer" should "properly consume delete events" in withDatabase { db ⇒

    db.connection.sendQuery(Queries.DELETE.statement)

    log.info("Waiting for binary log event to arrive.")
    val mutation = queue.poll(10, TimeUnit.SECONDS)

    // expect the row back
    assert(mutation != null)
    assert(mutation.isInstanceOf[DeleteMutation])
  }

  "A binlog consumer" should "not advance it's binlog position until a transaction is committed" in withDatabase { db ⇒

    queue.clear()

    val position1 = consumer.binaryLogPosition.get

    Await.result(db.connection.sendQuery(Queries.TX.BEGIN), 1.second)
    Await.result(db.connection.sendQuery(Queries.INSERT.statement), 1.second)

    queue.poll(10, TimeUnit.SECONDS)

    val position2 = consumer.binaryLogPosition.get

    Await.result(db.connection.sendQuery(Queries.TX.COMMIT), 1.second)

    // used to block
    Await.result(db.connection.sendQuery(Queries.INSERT.statement), 1.second)
    queue.poll(10, TimeUnit.SECONDS)

    val position3 = consumer.binaryLogPosition.get

    assert(position1.pos == position2.pos)
    assert(position2.pos < position3.pos)
  }

  "A binlog consumer" should "not advance it's binlog position until a transaction is rolled back" in withDatabase { db ⇒

    queue.clear()

    val position1 = consumer.binaryLogPosition.get

    Await.result(db.connection.sendQuery(Queries.TX.BEGIN), 1.second)
    Await.result(db.connection.sendQuery(Queries.INSERT.statement), 1.second)

    queue.poll(10, TimeUnit.SECONDS)

    val position2 = consumer.binaryLogPosition.get

    Await.result(db.connection.sendQuery(Queries.TX.ROLLBACK), 1.second)

    // used to block
    Await.result(db.connection.sendQuery(Queries.INSERT.statement), 1.second)
    queue.poll(10, TimeUnit.SECONDS)

    val position3 = consumer.binaryLogPosition.get

    assert(position1.pos == position2.pos)
    assert(position2.pos < position3.pos)
  }
}
