package mypipe.producer.cassandra

import akka.actor.{ Actor, ActorSystem, Props }
import akka.pattern.ask
import scala.concurrent.duration._
import com.netflix.astyanax.{ Serializer, Keyspace, AstyanaxContext, MutationBatch }
import mypipe.Conf
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
import com.typesafe.config.Config

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
    Log.info(s"Flush ${CassandraMapping.mutations.size} mutations.")

    val results = CassandraMapping.mutations.map(m ⇒ {
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

    CassandraMapping.mutations.clear()
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

case class CassandraProducer(mappings: List[Mapping], config: Config) extends Producer(mappings, config) {

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

  mappings.foreach(_.asInstanceOf[CassandraMapping].clusterConfig = clusterConfig)

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

  override def toString(): String = {
    // TODO: this needs to be able to return the cassandra cluster address / name
    "CassandraProducer"
  }
}

case class CassandraClusterConfig(clusterName: String, port: Int, seeds: String, maxConnsPerHost: Int = 1)

trait CassandraMapping extends Mapping {

  var clusterConfig: CassandraClusterConfig = null

  val columnFamilies = scala.collection.mutable.HashMap[String, ColumnFamily[_, _]]()

  def columnFamily[R, C](name: String, keySer: Serializer[R], colSer: Serializer[C]): ColumnFamily[R, C] = {
    columnFamilies.getOrElseUpdate(name, createColumnFamily[R, C](name, keySer, colSer)).asInstanceOf[ColumnFamily[R, C]]
  }

  def mutation(keyspace: String): MutationBatch = {
    CassandraMapping.mutations.getOrElseUpdate(keyspace, CassandraMapping.createMutation(keyspace, clusterConfig))
  }

  protected def createColumnFamily[R, C](cf: String, keySer: Serializer[R], colSer: Serializer[C]): ColumnFamily[R, C] = {
    new ColumnFamily[R, C](cf, keySer, colSer)
  }
}

object CassandraMapping {
  val TIMEUUID = TimeUUIDSerializer.get()
  val STRING = StringSerializer.get()
  val LONG = LongSerializer.get()

  val keyspaces = scala.collection.mutable.HashMap[String, Keyspace]()
  val mutations = scala.collection.mutable.HashMap[String, MutationBatch]()

  protected def createMutation(keyspace: String, clusterConfig: CassandraClusterConfig): MutationBatch = {
    val ks = keyspaces.getOrElseUpdate(keyspace, createKeyspace(keyspace, clusterConfig))
    ks.prepareMutationBatch()
  }

  protected def createKeyspace(keyspace: String, clusterConfig: CassandraClusterConfig): Keyspace = {

    val context: AstyanaxContext[Keyspace] = new AstyanaxContext.Builder()
      .forCluster(clusterConfig.clusterName)
      .forKeyspace(keyspace)
      .withAstyanaxConfiguration(new AstyanaxConfigurationImpl()
        .setDiscoveryType(NodeDiscoveryType.RING_DESCRIBE))
      .withConnectionPoolConfiguration(new ConnectionPoolConfigurationImpl(s"${clusterConfig.clusterName}-$keyspace-connpool")
        .setPort(clusterConfig.port)
        .setMaxConnsPerHost(clusterConfig.maxConnsPerHost)
        .setSeeds(clusterConfig.seeds))
      .withConnectionPoolMonitor(new CountingConnectionPoolMonitor())
      .buildKeyspace(ThriftFamilyFactory.getInstance())

    context.start()
    context.getClient()
  }
}