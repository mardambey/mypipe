package mypipe.producer.cassandra

import akka.actor.{ Actor, ActorSystem, Props }
import akka.pattern.ask
import scala.concurrent.duration._
import com.netflix.astyanax.MutationBatch
import mypipe.{ Conf, Log }
import scala.concurrent.Await
import mypipe.api._
import mypipe.api.DeleteMutation
import mypipe.api.UpdateMutation
import mypipe.api.InsertMutation

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
          println(s"InsertMutation: table=${i.table.name} values=$row")
        })
      }

      case u: UpdateMutation ⇒ {
        u.rows.foreach(row ⇒ {
          val old = row._1
          val cur = row._2
          println(s"UpdateMutation: table=${u.table.name} old=$old, cur=$cur")
        })
      }

      case d: DeleteMutation ⇒ {
        d.rows.foreach(row ⇒ {
          println(s"DeleteMutation: table=${d.table.name} values=$row")
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

