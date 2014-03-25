package mypipe.producer.cassandra

import com.netflix.astyanax.thrift.ThriftFamilyFactory
import com.netflix.astyanax.connectionpool.impl.{ ConnectionPoolConfigurationImpl, CountingConnectionPoolMonitor }
import com.netflix.astyanax.connectionpool.NodeDiscoveryType
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl
import com.netflix.astyanax.{ Serializer, MutationBatch, Keyspace, AstyanaxContext }
import com.netflix.astyanax.serializers.{ TimeUUIDSerializer, StringSerializer, LongSerializer, IntegerSerializer }
import com.netflix.astyanax.model.ColumnFamily
import mypipe.api.Mapping

case class CassandraClusterConfig(clusterName: String, port: Int, seeds: String, maxConnsPerHost: Int = 1)

trait CassandraMapping extends Mapping {

  var mutations: scala.collection.mutable.HashMap[String, MutationBatch] = null
  var clusterConfig: CassandraClusterConfig = null

  val columnFamilies = scala.collection.mutable.HashMap[String, ColumnFamily[_, _]]()

  def columnFamily[R, C](name: String, keySer: Serializer[R], colSer: Serializer[C]): ColumnFamily[R, C] = {
    columnFamilies.getOrElseUpdate(name, createColumnFamily[R, C](name, keySer, colSer)).asInstanceOf[ColumnFamily[R, C]]
  }

  def mutation(keyspace: String): MutationBatch = {
    mutations.getOrElseUpdate(keyspace, CassandraMapping.createMutation(keyspace, clusterConfig))
  }

  protected def createColumnFamily[R, C](cf: String, keySer: Serializer[R], colSer: Serializer[C]): ColumnFamily[R, C] = {
    new ColumnFamily[R, C](cf, keySer, colSer)
  }
}

object CassandraMapping {
  val TIMEUUID = TimeUUIDSerializer.get()
  val STRING = StringSerializer.get()
  val LONG = LongSerializer.get()
  val INT = IntegerSerializer.get()

  val keyspaces = scala.collection.mutable.HashMap[String, Keyspace]()

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

