package mypipe.kafka

import org.slf4j.LoggerFactory
import scala.concurrent._
import ExecutionContext.Implicits.global
import mypipe.avro.{ GenericInMemorySchemaRepo, AvroVersionedRecordDeserializer }
import kafka.consumer.{ ConsumerConfig, Consumer ⇒ KConsumer, ConsumerConnector }
import java.util.Properties
import scala.annotation.tailrec
import java.nio.ByteBuffer
import mypipe.avro.schema.GenericSchemaRepository
import org.apache.avro.Schema

abstract class KafkaConsumer(topic: String, zkConnect: String, groupId: String) {

  type INPUT = Array[Byte]

  protected val log = LoggerFactory.getLogger(getClass.getName)
  protected var future: Future[Unit] = _
  @volatile protected var loop = true

  protected val consumerConnector = createConsumerConnector(zkConnect, groupId)
  protected val mapStreams = consumerConnector.createMessageStreams(Map(topic -> 1))
  protected val stream = mapStreams.get(topic).get.head
  protected val consumerIterator = stream.iterator()

  @tailrec private def consume {
    val message = consumerIterator.next()

    if (message != null) {
      val next = try {
        onEvent(message.message())
      } catch {
        case e: Exception ⇒ log.error("Failed deserializing or processing message."); false
      }

      if (next && loop) consume
    } else {
      Unit
    }
  }

  def start: Future[Unit] = {

    future = Future {
      try {
        consume
        stop
      } catch {
        case e: Exception ⇒ stop
      }
    }

    future
  }

  def stop {
    loop = false
    consumerConnector.shutdown()
  }

  def onEvent(bytes: Array[Byte]): Boolean

  protected def createConsumerConnector(zkConnect: String, groupId: String): ConsumerConnector = {
    val props = new Properties()
    props.put("zookeeper.connect", zkConnect)
    props.put("group.id", groupId)
    props.put("zookeeper.session.timeout.ms", "400")
    props.put("zookeeper.sync.time.ms", "200")
    props.put("auto.commit.interval.ms", "1000")
    KConsumer.create(new ConsumerConfig(props))
  }
}

/** Consumes specific Avro records from Kafka.
 *
 *  @param topic
 *  @param zkConnect
 *  @param groupId
 */
abstract class KafkaGenericMutationAvroConsumer[SchemaId](
  topic: String,
  zkConnect: String,
  groupId: String,
  schemaIdSizeInBytes: Int)(insertCallback: (mypipe.avro.InsertMutation) ⇒ Boolean,
                            updateCallback: (mypipe.avro.UpdateMutation) ⇒ Boolean,
                            deleteCallback: (mypipe.avro.DeleteMutation) ⇒ Boolean)
    extends KafkaConsumer(topic, zkConnect, groupId) {

  // abstract fields and methods
  protected val schemaRepoClient: GenericSchemaRepository[SchemaId, Schema]
  protected def bytesToSchemaId(bytes: Array[Byte], offset: Int): SchemaId

  protected val logger = LoggerFactory.getLogger(getClass.getName)
  protected val headerLength = PROTO_HEADER_LEN_V0 + schemaIdSizeInBytes

  lazy val insertDeserializer = new AvroVersionedRecordDeserializer[mypipe.avro.InsertMutation, SchemaId](schemaRepoClient)
  lazy val updateDeserializer = new AvroVersionedRecordDeserializer[mypipe.avro.UpdateMutation, SchemaId](schemaRepoClient)
  lazy val deleteDeserializer = new AvroVersionedRecordDeserializer[mypipe.avro.DeleteMutation, SchemaId](schemaRepoClient)

  override def onEvent(bytes: Array[Byte]): Boolean = try {

    val magicByte = bytes(0)

    if (magicByte != PROTO_MAGIC_V0) {
      logger.error(s"We have encountered an unknown magic byte! Magic Byte: $magicByte")
      false
    } else {
      val mutationType = bytes(1)
      val schemaId = bytesToSchemaId(bytes, PROTO_HEADER_LEN_V0)

      val continue = mutationType match {

        case PROTO_INSERT ⇒
          insertDeserializer.deserialize("insert", schemaId, bytes, headerLength)
            .map(insertCallback(_))
        case PROTO_UPDATE ⇒
          updateDeserializer.deserialize("update", schemaId, bytes, headerLength)
            .map(updateCallback)
        case PROTO_DELETE ⇒
          deleteDeserializer.deserialize("delete", schemaId, bytes, headerLength)
            .map(deleteCallback)
      }

      continue.getOrElse(false)
    }
  } catch {
    case e: Exception ⇒
      log.error("Could not run callback on " + bytes + " => " + e.getMessage + "\n" + e.getStackTraceString)
      false
  }
}

