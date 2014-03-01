package mypipe.producer.cassandra

import akka.actor.{ Actor, ActorSystem, Props }
import akka.pattern.ask
import scala.concurrent.duration._
import com.netflix.astyanax.{ Serializer, Keyspace, AstyanaxContext, MutationBatch }
import mypipe.{ Conf, Log }
import scala.concurrent.Await
import mypipe.api._
import mypipe.api.DeleteMutation
import mypipe.api.UpdateMutation
import mypipe.api.InsertMutation
import com.netflix.astyanax.serializers.{ TimeUUIDSerializer, LongSerializer, StringSerializer }
import com.netflix.astyanax.thrift.ThriftFamilyFactory
import com.netflix.astyanax.connectionpool.impl.{ ConnectionPoolConfigurationImpl, CountingConnectionPoolMonitor }
import com.netflix.astyanax.connectionpool.NodeDiscoveryType
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl
import com.netflix.astyanax.model.ColumnFamily

case class Queue(mutation: Mutation[_])
case class QueueList(mutations: List[Mutation[_]])
case object Flush

class CassandraBatchWriter(mappings: List[Mapping]) extends Actor {

  def receive = {
    case Queue(mutation)      ⇒ map(mutation)
    case QueueList(mutationz) ⇒ map(mutationz)
    case Flush                ⇒ sender ! flush()
  }

  def flush(): Boolean = {
    Log.info(s"Flush ${CassandraMappings.mutations.size} mutations.")

    val results = CassandraMappings.mutations.map(m ⇒ {
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

    CassandraMappings.mutations.clear()
    true
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
  def props(mappings: List[Mapping]): Props = Props(new CassandraBatchWriter(mappings))
}

case class CassandraProducer(mappings: List[Mapping]) extends Producer(mappings) {

  val system = ActorSystem("mypipe")
  val worker = system.actorOf(CassandraBatchWriter.props(mappings), "CassandraBatchWriterActor")

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

object CassandraMappings {

  val CASSANDRA_CLUSTER_NAME = Conf.conf.getString("mypipe.producers.cassandra.cluster.name")
  val CASSANDRA_SEEDS = Conf.conf.getString("mypipe.producers.cassandra.cluster.seeds")
  val CASSANDRA_PORT = Conf.conf.getInt("mypipe.producers.cassandra.cluster.port")
  val CASSANDRA_MAX_CONNS_PER_HOST = Conf.conf.getInt("mypipe.producers.cassandra.cluster.max-conns-per-host")

  val keyspaces = scala.collection.mutable.HashMap[String, Keyspace]()
  val columnFamilies = scala.collection.mutable.HashMap[String, ColumnFamily[_, _]]()
  val mutations = scala.collection.mutable.HashMap[String, MutationBatch]()

  object Serializers {
    val TIMEUUID = TimeUUIDSerializer.get()
    val STRING = StringSerializer.get()
    val LONG = LongSerializer.get()
  }

  def columnFamily[R, C](name: String, keySer: Serializer[R], colSer: Serializer[C]): ColumnFamily[R, C] = {
    columnFamilies.getOrElseUpdate(name, createColumnFamily[R, C](name, keySer, colSer)).asInstanceOf[ColumnFamily[R, C]]
  }

  def mutation(keyspace: String): MutationBatch = {
    mutations.getOrElseUpdate(keyspace, createMutation(keyspace))
  }

  protected def createMutation(keyspace: String): MutationBatch = {
    val ks = keyspaces.getOrElseUpdate(keyspace, createKeyspace(keyspace))
    ks.prepareMutationBatch()
  }

  protected def createColumnFamily[R, C](cf: String, keySer: Serializer[R], colSer: Serializer[C]): ColumnFamily[R, C] = {
    new ColumnFamily[R, C](cf, keySer, colSer)
  }

  protected def createKeyspace(keyspace: String): Keyspace = {

    val context: AstyanaxContext[Keyspace] = new AstyanaxContext.Builder()
      .forCluster(CASSANDRA_CLUSTER_NAME)
      .forKeyspace(keyspace)
      .withAstyanaxConfiguration(new AstyanaxConfigurationImpl()
        .setDiscoveryType(NodeDiscoveryType.RING_DESCRIBE))
      .withConnectionPoolConfiguration(new ConnectionPoolConfigurationImpl(s"$CASSANDRA_CLUSTER_NAME-$keyspace-connpool")
        .setPort(CASSANDRA_PORT)
        .setMaxConnsPerHost(CASSANDRA_MAX_CONNS_PER_HOST)
        .setSeeds(CASSANDRA_SEEDS))
      .withConnectionPoolMonitor(new CountingConnectionPoolMonitor())
      .buildKeyspace(ThriftFamilyFactory.getInstance())

    context.start()
    context.getClient()
  }
}