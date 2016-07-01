package mypipe.kafka.producer

import mypipe.api.data.Row
import mypipe.api.event.{Mutation, SingleValuedMutation, UpdateMutation}
import mypipe.api.producer.Producer
import com.typesafe.config.Config
import org.apache.avro.specific.SpecificRecord
import org.apache.avro.io.EncoderFactory

import org.apache.kafka.common.serialization.Serializer
import org.slf4j.LoggerFactory

import KafkaMutationAvroProducer.MessageType

object KafkaMutationAvroProducer {
  type MessageType = (Mutation, Either[Row, (Row, Row)])
}

/** The base class for a Mypipe producer that encodes Mutation instances
 *  as Avro records and publishes them into Kafka.
 *
 *  @param config configuration must have "metadata-brokers"
 */
abstract class KafkaMutationAvroProducer[T <: Serializer[MessageType]](config: Config, serializerClass: Class[T], producerProperties: Map[AnyRef, AnyRef] = Map.empty)
    extends Producer(config = config) {

  type InputRecord = SpecificRecord
  type OutputType = Array[Byte]

  protected val metadataBrokers = config.getString("metadata-brokers")

  protected val producer = new KafkaProducer(metadataBrokers, serializerClass, producerProperties)

  protected val logger = LoggerFactory.getLogger(getClass)
  protected val encoderFactory = EncoderFactory.get()

  /** Builds the Kafka topic using the mutation's database, table name, and
   *  the mutation type (insert, update, delete) concatenating them with "_"
   *  as a delimiter.
   *
   *  @param mutation mutation
   *  @return the topic name
   */
  protected def getKafkaTopic(mutation: Mutation): String

  override def flush(): Boolean = {
    try {
      producer.flush
      true
    } catch {
      case e: Exception ⇒
        logger.error(s"Could not flush producer queue: ${e.getMessage} -> ${e.getStackTraceString}")
        false
    }
  }

  override def queueList(inputList: List[Mutation]): Boolean = {
    inputList.dropWhile(queue).isEmpty
  }

  override def queue(input: Mutation): Boolean = {
    try {
      if (input.isInstanceOf[SingleValuedMutation]) {
        val mut = input.asInstanceOf[SingleValuedMutation]
        mut.rows foreach { row ⇒ producer.queue(getKafkaTopic(input), (mut, Left(row))) }
        true
      } else {
        val mut = input.asInstanceOf[UpdateMutation]
        mut.rows foreach { row ⇒ producer.queue(getKafkaTopic(input), (mut, Right(row._1, row._2))) }
        true
      }
    } catch {
      case e: Exception ⇒
        logger.error(s"failed to queue: ${e.getMessage}\n${e.getStackTraceString}")
        false
    }
  }

  override def toString: String = {
    s"kafka-avro-producer-$metadataBrokers"
  }
}

