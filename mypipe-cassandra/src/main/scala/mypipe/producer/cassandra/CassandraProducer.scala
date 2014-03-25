package mypipe.producer.cassandra

import akka.actor.ActorSystem
import akka.pattern.ask
import scala.concurrent.duration._
import com.netflix.astyanax.MutationBatch
import mypipe.Conf
import scala.concurrent.Await
import mypipe.api._
import com.typesafe.config.Config

case class CassandraProducer(mappings: List[Mapping], config: Config) extends Producer(mappings, config) {

  val mutations = scala.collection.mutable.HashMap[String, MutationBatch]()

  val clusterConfig = try {
    val CASSANDRA_CLUSTER_NAME = config.getString("cluster.name")
    val CASSANDRA_SEEDS = config.getString("cluster.seeds")
    val CASSANDRA_PORT = config.getInt("cluster.port")
    val CASSANDRA_MAX_CONNS_PER_HOST = config.getInt("cluster.max-conns-per-host")
    CassandraClusterConfig(CASSANDRA_CLUSTER_NAME, CASSANDRA_PORT, CASSANDRA_SEEDS, CASSANDRA_MAX_CONNS_PER_HOST)
  } catch {
    case e: Exception ⇒ {
      Log.severe(s"Error connecting to Cassandra cluster: ${e.getMessage} -> ${e.getStackTraceString}")
      CassandraClusterConfig("NoName", 0, "NoSeeds", 1)
    }
  }

  mappings.foreach(m ⇒ {
    m.asInstanceOf[CassandraMapping].clusterConfig = clusterConfig
    m.asInstanceOf[CassandraMapping].mutations = mutations
  })

  val system = ActorSystem("mypipe")
  val worker = system.actorOf(CassandraBatchWriter.props(mappings, mutations), "CassandraBatchWriterActor")

  def queue(mutation: Mutation[_]): Boolean = {
    worker ! Queue(mutation)
    true
  }

  def queueList(mutations: List[Mutation[_]]): Boolean = {
    worker ! QueueList(mutations)
    true
  }

  def flush(): Boolean = {
    val future = worker.ask(Flush)(Conf.SHUTDOWN_FLUSH_WAIT_SECS seconds)
    Await.result(future, Conf.SHUTDOWN_FLUSH_WAIT_SECS seconds).asInstanceOf[Boolean]
  }

  override def toString(): String = {
    s"CassandraProducer/${clusterConfig.clusterName}"
  }
}

