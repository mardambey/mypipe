package mypipe.kafka.producer

import com.typesafe.config.Config
import mypipe.api.event.{ AlterEvent, Mutation }
import mypipe.kafka.KafkaUtil

class KafkaMutationSpecificAvroProducer(config: Config) extends KafkaMutationAvroProducer(config, classOf[mypipe.kafka.producer.KafkaSpecificAvroSerializer], Map("schema-repo-client" -> config.getString("schema-repo-client"))) {

  override def handleAlter(event: AlterEvent): Boolean = {
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
