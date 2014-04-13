package mypipe.kafka

import mypipe.api._
import org.slf4j.LoggerFactory
import scala.concurrent._
import ExecutionContext.Implicits.global
import scala.reflect.runtime.universe.TypeTag
import mypipe.avro.{ GenericInMemorySchemaRepo, AvroVersionedRecordDeserializer }
import kafka.consumer.{ ConsumerIterator, ConsumerConfig, Consumer ⇒ KConsumer, ConsumerConnector }
import java.util.Properties
import org.apache.avro.Schema
import mypipe.avro.schema.SchemaRepository
import org.apache.avro.specific.SpecificRecord
import scala.annotation.tailrec
import java.nio.ByteBuffer

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
class KafkaGenericMutationAvroConsumer(topic: String, zkConnect: String, groupId: String)(insertCallback: (mypipe.avro.InsertMutation) ⇒ Boolean,
                                                                                          updateCallback: (mypipe.avro.UpdateMutation) ⇒ Boolean,
                                                                                          deleteCallback: (mypipe.avro.DeleteMutation) ⇒ Boolean)
    extends KafkaConsumer(topic, zkConnect, groupId) {

  protected val schemaIdLength = 2 // schema ID is a Short
  protected val logger = LoggerFactory.getLogger(getClass.getName)
  protected val schemaRepoClient = GenericInMemorySchemaRepo
  protected val headerLength = PROTO_HEADER_LEN_V0 + schemaIdLength

  val insertDeserializer = new AvroVersionedRecordDeserializer[mypipe.avro.InsertMutation](schemaRepoClient)
  val updateDeserializer = new AvroVersionedRecordDeserializer[mypipe.avro.UpdateMutation](schemaRepoClient)
  val deleteDeserializer = new AvroVersionedRecordDeserializer[mypipe.avro.DeleteMutation](schemaRepoClient)

  override def onEvent(inputRecord: Array[Byte]): Boolean = try {

    val buf = ByteBuffer.wrap(inputRecord)
    val magicByte = buf.get()

    if (magicByte != PROTO_MAGIC_V0) {
      logger.error(s"We have encountered an unknown magic byte! Magic Byte: $magicByte")
      false
    } else {
      val mutationType = buf.get()
      val schemaId: Short = buf.getShort

      val continue = mutationType match {

        case PROTO_INSERT ⇒
          insertDeserializer.deserialize("insert", schemaId, inputRecord, headerLength)
            .map(insertCallback(_))
        case PROTO_UPDATE ⇒
          updateDeserializer.deserialize("update", schemaId, inputRecord, headerLength)
            .map(updateCallback)
        case PROTO_DELETE ⇒
          deleteDeserializer.deserialize("delete", schemaId, inputRecord, headerLength)
            .map(deleteCallback)
      }

      continue.getOrElse(false)
    }
  } catch {
    case e: Exception ⇒
      log.error("Could not run callback on " + inputRecord + " => " + e.getMessage + "\n" + e.getStackTraceString)
      false
  }
}

