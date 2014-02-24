package mypipe.producer.cassandra

import com.netflix.astyanax.MutationBatch
import mypipe.api.{ DeleteMutation, UpdateMutation, InsertMutation }

case class KS(keySpace: String, clusterName: String, seeds: String = "127.0.0.1:9160", port: Int = 9160, maxConnsPerHost: Int = 1) {

  //val context: AstyanaxContext[Keyspace] = new AstyanaxContext.Builder()
  //  .forCluster(clusterName)
  //  .forKeyspace(keySpace)
  //  .withAstyanaxConfiguration(new AstyanaxConfigurationImpl()
  //    .setDiscoveryType(NodeDiscoveryType.RING_DESCRIBE))
  //  .withConnectionPoolConfiguration(new ConnectionPoolConfigurationImpl(s"$clusterName-$keySpace-connpool")
  //    .setPort(port)
  //    .setMaxConnsPerHost(maxConnsPerHost)
  //    .setSeeds(seeds))
  //  .withConnectionPoolMonitor(new CountingConnectionPoolMonitor())
  //  .buildKeyspace(ThriftFamilyFactory.getInstance())

  //context.start()

  //val keyspace = context.getClient()

  def CF[K, V](cf: String) = new CF(this, cf)
}

case class Row(cf: CF, key: String) {

  def put(kv: (String, String)): Row = {
    this
  }

  def incr(key: String): Row = {
    this
  }

  def decr(key: String): Row = {
    this
  }
}

case class CF(ks: KS, cf: String) {

  //val CF_USER_INFO: ColumnFamily[String, String] =
  //  new ColumnFamily[String, String](
  //    "Standard1", // Column Family Name
  //    StringSerializer.get(), // Key Serializer
  //    StringSerializer.get()) // Column Serializer

  def row(rowKey: String): Row = {
    Row(this, rowKey)
  }
}

trait Mapping[T] {
  def map(mutation: InsertMutation): T
  def map(mutation: UpdateMutation): T
  def map(mutation: DeleteMutation): T
}

class CassandraProfileMapping extends Mapping[MutationBatch] {

  val ks = KS("TestKeySpace", "TestCluster")
  val profiles = ks.CF[String, String]("profiles")
  val counters = ks.CF[Long, String]("counters")

  def map(mutation: UpdateMutation): MutationBatch = ???
  def map(mutation: DeleteMutation): MutationBatch = ???
  def map(mutation: InsertMutation): MutationBatch = {

    profiles
      .row("1501571")
      .put("nick_name" -> "hisham320")
      .put("email" -> "hmb@mate1.com")

    counters
      .row("1501571")
      .incr("login_count")
      .decr("free_offers")

    null
  }
}

object CassandraDSLTest extends App {

}