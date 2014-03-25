package mypipe.producer.cassandra

import mypipe.api._
import scala.collection.mutable
import com.netflix.astyanax.MutationBatch
import akka.actor.{ Props, Actor }
import mypipe.api.InsertMutation
import scala.Some

case class Queue(mutation: Mutation[_])
case class QueueList(mutations: List[Mutation[_]])
case object Flush

class CassandraBatchWriter(mappings: List[Mapping], mutations: mutable.HashMap[String, MutationBatch]) extends Actor {

  def receive = {
    case Queue(mutation)      ⇒ map(mutation)
    case QueueList(mutationz) ⇒ map(mutationz)
    case Flush                ⇒ sender ! flush()
  }

  def flush(): Boolean = {
    Log.info(s"Flush ${mutations.size} mutations.")

    val results = mutations.map(m ⇒ {
      try {
        // TODO: use execute async
        Some(m._2.execute())
      } catch {
        case e: Exception ⇒ {
          Log.severe(s"Could not execute mutation batch: ${e.getMessage} -> ${e.getStackTraceString}")
          None
        }
      }
    })

    // TODO: what do we do when we fail?
    mutations.clear()
    // check for None's, if so, we've failed
    results.filter(_.isEmpty).size == 0
  }

  def map(mutations: List[Mutation[_]]) {
    mutations.map(m ⇒ map(m))
  }

  def map(mutation: Mutation[_]) {

    mutation match {
      case i: InsertMutation ⇒ mappings.map(m ⇒ m.map(i))
      case u: UpdateMutation ⇒ mappings.map(m ⇒ m.map(u))
      case d: DeleteMutation ⇒ mappings.map(m ⇒ m.map(d))
    }
  }
}

object CassandraBatchWriter {
  def props(mappings: List[Mapping], mutations: collection.mutable.HashMap[String, MutationBatch]): Props = Props(new CassandraBatchWriter(mappings, mutations))
}