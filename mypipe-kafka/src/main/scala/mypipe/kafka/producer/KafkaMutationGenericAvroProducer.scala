package mypipe.kafka.producer

import com.typesafe.config.Config
import mypipe.api.event._
import mypipe.kafka.KafkaUtil

object KafkaMutationGenericAvroProducer {
  def apply(config: Config) = new KafkaMutationGenericAvroProducer(config)
}

/** An implementation of the base KafkaMutationAvroProducer class that uses a
 *  GenericInMemorySchemaRepo in order to encode mutations as Avro beans.
 *  Three beans are encoded:
 *  InsertMutation
 *  UpdateMutation
 *  DeleteMutation
 *
 *  The Kafka topic names are calculated as:
 *  dbName_tableName_(insert|update|delete)
 *
 *  @param config configuration must have "metadata-brokers"
 */
class KafkaMutationGenericAvroProducer(config: Config) extends KafkaMutationAvroProducer(config, classOf[mypipe.kafka.producer.KafkaGenericAvroSerializer], Map("schema-repo-client" -> "mypipe.avro.GenericInMemorySchemaRepo")) {

  override def handleAlter(event: AlterEvent): Boolean = {
    // no special support for alters needed, "generic" schema
    true
  }

  /** Builds the Kafka topic using the mutation's database, table name, and
   *  the mutation type (insert, update, delete) concatenating them with "_"
   *  as a delimeter.
   *
   *  @param mutation mutation
   *  @return the topic name
   */
  override protected def getKafkaTopic(mutation: Mutation): String =
    KafkaUtil.genericTopic(mutation)
}
