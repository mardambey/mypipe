package mypipe

import mypipe.mysql._
import scala.concurrent.{ Future, Await }
import scala.concurrent.duration._
import mypipe.api._
import mypipe.producer.QueueProducer
import java.util.concurrent.{ TimeUnit, LinkedBlockingQueue }
import akka.actor.ActorDSL._
import akka.pattern.ask
import org.scalatest.BeforeAndAfterAll
import mypipe.api.InsertMutation
import akka.util.Timeout
import akka.agent.Agent
import scala.collection.mutable.ListBuffer
import org.slf4j.LoggerFactory

class LatencySpec extends UnitSpec with DatabaseSpec with ActorSystemSpec with BeforeAndAfterAll {

  @volatile var connected = false

  val log = LoggerFactory.getLogger(getClass)
  val maxLatency = conf.getLong("mypipe.test.max-latency")
  val latencies = ListBuffer[Long]()

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

  val maxId = Agent(0)
  val insertQueue = new LinkedBlockingQueue[(Int, Long)]()
  val binlogQueue = new LinkedBlockingQueue[Mutation[_]]()

  case object Insert
  case object Consume
  case object Quit

  "Mypipe" should s"consume messages with a latency lower than $maxLatency millis" in {

    // actor1:
    // add row into local queue
    // add row into mysql
    val insertProducer = actor(new Act {

      var id = 1

      become {
        case Insert ⇒ {

          try {
            val f = db.connection.sendQuery(Queries.INSERT.statement(id = id.toString))
            Await.result(f, 1000 millis)

            maxId.alter(id)
            insertQueue.add((id, System.nanoTime()))
            id += 1
          } catch { case t: Throwable ⇒ }

          self ! Insert
        }

        case Quit ⇒ sender ! true
      }
    })

    // actor2:
    // consumes binlogs from the server and puts the results
    // in a local queue for other actors to process
    val binlogConsumer = actor(new Act {

      val queueProducer = new QueueProducer(binlogQueue)
      val consumer = MySQLBinaryLogConsumer(Queries.DATABASE.host, Queries.DATABASE.port, Queries.DATABASE.username, Queries.DATABASE.password, BinaryLogFilePosition.current)

      consumer.registerListener(new BinaryLogConsumerListener() {
        override def onMutation(c: AbstractBinaryLogConsumer, mutation: Mutation[_]): Boolean = {
          queueProducer.queue(mutation)
          true
        }

        override def onMutation(c: AbstractBinaryLogConsumer, mutations: Seq[Mutation[_]]): Boolean = {
          queueProducer.queueList(mutations.toList)
          true
        }

        override def onConnect(c: AbstractBinaryLogConsumer) { connected = true }
      })

      val f = Future { consumer.connect() }

      become {

        case Quit ⇒ {
          consumer.disconnect()
          Await.result(f, 5 seconds)
          sender ! true
        }
      }

    })

    // actor3:
    // poll local queue and get inserted latest event
    // wait on binlog consumer to hand us back the same event
    // calculate latency
    val insertConsumer = actor(new Act {

      become {
        case Consume ⇒ {

          val id = insertQueue.poll(1, TimeUnit.SECONDS)
          var found = false

          while (!found) {

            val mutation = binlogQueue.poll(1, TimeUnit.SECONDS)

            if (mutation.isInstanceOf[InsertMutation]) {

              val colName = mutation.table.primaryKey.get.columns.head.name
              val primaryKey = mutation.asInstanceOf[InsertMutation].rows.head.columns(colName).value[Int]

              if (id._1 == primaryKey) {
                latencies += System.nanoTime() - id._2
                found = true
              } else {
                log.debug(s"Did not find a matching mutation with id = $id, cur val is $primaryKey, will keep looking.")
              }

            }
          }

          self ! Consume

        }

        case Quit ⇒ sender ! true
      }
    })

    binlogConsumer ! Consume

    while (!connected) Thread.sleep(1)

    insertConsumer ! Consume
    insertProducer ! Insert

    while (maxId.get() < 100) {
      Thread.sleep(1)
    }

    val future = Future.sequence(List(ask(insertProducer, Quit), ask(binlogConsumer, Quit), ask(insertConsumer, Quit)))

    try {
      Await.result(future, 30 seconds)
    } catch {
      case e: Exception ⇒ log.debug("Timed out waiting for actors to shutdown, proceeding anyway.")
    }

    system.stop(insertProducer)
    system.stop(binlogConsumer)
    system.stop(insertConsumer)

    val latency = latencies.fold(0L)(_ + _) / latencies.size

    println(s"Latency: ${latency / 1000000.0} millis ($latency nanos)")
    assert(latency / 1000000.0 < maxLatency)
  }
}
