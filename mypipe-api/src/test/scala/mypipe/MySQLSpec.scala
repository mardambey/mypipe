package mypipe

import com.typesafe.config.ConfigFactory
import mypipe.api.Conf
import mypipe.api.event.{ DeleteMutation, Mutation, UpdateMutation, InsertMutation }
import mypipe.api.repo.FileBasedBinaryLogPositionRepository
import mypipe.mysql.MySQLBinaryLogConsumer
import mypipe.pipe.Pipe
import scala.concurrent.Await
import scala.concurrent.duration._
import mypipe.producer.QueueProducer
import java.util.concurrent.{ TimeUnit, LinkedBlockingQueue }
import org.slf4j.LoggerFactory

class MySQLSpec extends UnitSpec with DatabaseSpec with ActorSystemSpec {

  val log = LoggerFactory.getLogger(getClass)

  val queue = new LinkedBlockingQueue[Mutation]()
  val queueProducer = new QueueProducer(queue)
  val c = ConfigFactory.parseString(
    s"""
       |{
       |  source = "${Queries.DATABASE.host}:${Queries.DATABASE.port}:${Queries.DATABASE.username}:${Queries.DATABASE.password}"
       |}
         """.stripMargin)
  val consumer = MySQLBinaryLogConsumer(c)
  val binlogPosRepo = new FileBasedBinaryLogPositionRepository(filePrefix = "test-pipe", dataDir = Conf.DATADIR)
  val pipe = new Pipe("test-pipe", consumer, queueProducer, binlogPosRepo)

  override def beforeAll() {
    super.beforeAll()
    pipe.connect()
    while (!pipe.isConnected) { Thread.sleep(10) }
  }

  override def afterAll() {
    pipe.disconnect()
    super.afterAll()
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

    val position1 = consumer.position.get

    Await.result(db.connection.sendQuery(Queries.TX.BEGIN), 1.second)
    Await.result(db.connection.sendQuery(Queries.INSERT.statement), 1.second)

    queue.poll(10, TimeUnit.SECONDS)

    val position2 = consumer.position.get

    Await.result(db.connection.sendQuery(Queries.TX.COMMIT), 1.second)

    // used to block
    Await.result(db.connection.sendQuery(Queries.INSERT.statement), 1.second)
    queue.poll(10, TimeUnit.SECONDS)

    val position3 = consumer.position.get

    assert(position1.pos == position2.pos)
    assert(position2.pos < position3.pos)
  }

  "A binlog consumer" should "not advance it's binlog position until a transaction is rolled back" in withDatabase { db ⇒

    queue.clear()

    val position1 = consumer.position.get

    Await.result(db.connection.sendQuery(Queries.TX.BEGIN), 1.second)
    Await.result(db.connection.sendQuery(Queries.INSERT.statement), 1.second)

    queue.poll(10, TimeUnit.SECONDS)

    val position2 = consumer.position.get

    Await.result(db.connection.sendQuery(Queries.TX.ROLLBACK), 1.second)

    // used to block
    Await.result(db.connection.sendQuery(Queries.INSERT.statement), 1.second)
    queue.poll(10, TimeUnit.SECONDS)

    val position3 = consumer.position.get

    assert(position1.pos == position2.pos)
    assert(position2.pos < position3.pos)
  }
}
