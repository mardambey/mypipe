package mypipe.kafka

import mypipe.api._
import java.util.logging.Logger
import scala.concurrent._
import ExecutionContext.Implicits.global
import scala.reflect.runtime.universe.TypeTag
import mypipe.avro.AvroVersionedRecordDeserializer
import kafka.consumer.{ ConsumerIterator, ConsumerConfig, Consumer ⇒ KConsumer, ConsumerConnector }
import java.util.Properties
import org.apache.avro.Schema
import mypipe.avro.schema.SchemaRepository
import org.apache.avro.specific.SpecificRecord
import scala.annotation.tailrec

abstract class KafkaConsumer[OUTPUT](topic: String, zkConnect: String, groupId: String) {

  type INPUT = Array[Byte]

  protected val log = Logger.getLogger(getClass.getName)
  protected val deserializer: Deserializer[INPUT, OUTPUT]
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
        // topic here is wrong if this is using generic record,s it needs to be "insert" only

        val data = deserializer.deserialize(sanitizeTopicForDeserializer(topic), message.message())
        // TODO: handle failure better
        if (data.isDefined) onEvent(data.get) else true
      } catch {
        case e: Exception ⇒ log.severe("Failed deserializing or processing message."); false
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

  def onEvent(inputRecord: OUTPUT): Boolean
  protected def sanitizeTopicForDeserializer(topic: String): String

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
 *  @param schemaRepoClient
 *  @param callback
 *  @tparam InputRecord
 */
class KafkaAvroRecordConsumer[InputRecord <: SpecificRecord](topic: String, zkConnect: String, groupId: String, schemaRepoClient: SchemaRepository[Short, Schema])(callback: (InputRecord) ⇒ Boolean)(implicit tag: TypeTag[InputRecord])
    extends KafkaConsumer[InputRecord](topic, zkConnect, groupId) {

  override val deserializer = new AvroVersionedRecordDeserializer[InputRecord](schemaRepoClient)

  override protected def sanitizeTopicForDeserializer(topic: String): String = topic match {
    case t if t.endsWith("_insert") ⇒ "insert"
    case t if t.endsWith("_delete") ⇒ "delete"
    case t if t.endsWith("_update") ⇒ "update"
  }

  override def onEvent(inputRecord: InputRecord): Boolean = try {
    callback(inputRecord)
  } catch {
    case e: Exception ⇒
      log.severe("Could not run callback on " + inputRecord)
      false
  }
}

