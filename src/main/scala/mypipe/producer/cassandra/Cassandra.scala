package mypipe.producer.cassandra

import akka.actor.{ Actor, ActorSystem, Props }
import akka.pattern.ask
import scala.concurrent.duration._
import mypipe.producer._
import com.netflix.astyanax.MutationBatch
import mypipe.producer.DeleteMutation
import mypipe.producer.UpdateMutation
import mypipe.producer.InsertMutation
import mypipe.{ Conf, Log }
import scala.concurrent.Await

case class Queue(mutation: Mutation[_])
case class QueueList(mutations: List[Mutation[_]])
case object Flush

class CassandraBatchWriter extends Actor {

  implicit val ec = context.dispatcher
  val mutations = scala.collection.mutable.ListBuffer[MutationBatch]()
  val cancellable =
    context.system.scheduler.schedule(Conf.CASSANDRA_FLUSH_INTERVAL_SECS seconds,
      Conf.CASSANDRA_FLUSH_INTERVAL_SECS seconds,
      self,
      Flush)

  def receive = {
    case Queue(mutation)      ⇒ mutations += map(mutation)
    case QueueList(mutationz) ⇒ mutations += map(mutationz)
    case Flush                ⇒ sender ! flush()
  }

  def flush(): Boolean = {
    Log.warning(s"TODO: flush ${mutations.size} mutations.")

    // TODO: flush
    mutations.clear()
    true
  }

  def map(mutation: Mutation[_]): MutationBatch = {
    map(mutation, null)
    null
  }

  def map(mutations: List[Mutation[_]]): MutationBatch = {
    mutations.foreach(m ⇒ map(m, null))
    null
  }

  def map(mutation: Mutation[_], mutationBatch: MutationBatch) {

    mutation match {

      case i: InsertMutation ⇒ {
        i.rows.foreach(row ⇒ {
          println(s"""InsertMutation: row=${row.getClass.getSimpleName} values=${row.mkString(",")}""")
        })
      }

      case u: UpdateMutation ⇒ {
        u.rows.foreach(row ⇒ {
          val old = row._1
          val cur = row._2

          println(s"""UpdateMutation: old=${old.mkString(",")}, cur=${cur.mkString(",")}""")
        })
      }

      case d: DeleteMutation ⇒ {
        d.rows.foreach(row ⇒ {
          println(s"""DeleteMutation: row=${row.getClass.getSimpleName} values=${row.mkString(",")}""")
        })
      }
    }
  }
}

object CassandraBatchWriter {
  def props(): Props = Props(classOf[CassandraBatchWriter])
}

case class CassandraProducer extends Producer {

  val system = ActorSystem("mypipe")
  val worker = system.actorOf(CassandraBatchWriter.props(), "CassandraBatchWriterActor")

  def queue(mutation: Mutation[_]) {
    worker ! Queue(mutation)
  }

  def queueList(mutations: List[Mutation[_]]) {
    worker ! QueueList(mutations)
  }

  def flush() {
    val future = worker.ask(Flush)(Conf.SHUTDOWN_FLUSH_WAIT_SECS seconds)
    val result = Await.result(future, Conf.SHUTDOWN_FLUSH_WAIT_SECS seconds).asInstanceOf[Boolean]
  }
}

