package mypipe.kafka.producer

import com.typesafe.config.Config
import mypipe.api.event.{ AlterEvent, Mutation }
import mypipe.kafka.KafkaUtil

class KafkaMutationSpecificAvroProducer(config: Config) extends KafkaMutationAvroProducer(config, classOf[mypipe.kafka.producer.KafkaSpecificAvroSerializer], Map("schema-repo-client" -> config.getString("schema-repo-client"))) {

  // TODO: re-implement support for this
  override def handleAlter(event: AlterEvent): Boolean = {
    //// FIXME: if the table is not in the cache already, by it's ID, this will fail
    //// refresh insert, update, and delete schemas
    //(for (
    //  i ← schemaRepoClient.getLatestSchema(AvroSchemaUtils.specificSubject(event.database, event.table.name, Mutation.InsertString), flushCache = true);
    //  u ← schemaRepoClient.getLatestSchema(AvroSchemaUtils.specificSubject(event.database, event.table.name, Mutation.UpdateString), flushCache = true);
    //  d ← schemaRepoClient.getLatestSchema(AvroSchemaUtils.specificSubject(event.database, event.table.name, Mutation.DeleteString), flushCache = true)
    //) yield {
    //  true
    //}).getOrElse(false)
    println("\n\n----------->> TODO: handle alter!\n\n")
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
    KafkaUtil.specificTopic(mutation)
}
