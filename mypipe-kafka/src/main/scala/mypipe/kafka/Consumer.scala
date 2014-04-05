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

abstract class Consumer[INPUT, OUTPUT](topic: String) {

  protected val log = Logger.getLogger(getClass.getName)
  protected val iterator: Iterator[INPUT]
  protected val deserializer: Deserializer[INPUT, OUTPUT]
  protected var future: Future[Unit] = _
  @volatile protected var loop = true

  def start {

    future = Future {
      iterator.takeWhile(message ⇒ {
        val next = try {
          val data = deserializer.deserialize(topic, message)
          // TODO: handle failure better
          if (data.isDefined) onEvent(data.get) else true
        } catch {
          case e: Exception ⇒ log.severe("Failed deserializing or processing message."); false
        }

        next && loop
      })

      shutdown
    }

  }

  def stop {
    loop = false
  }

  def shutdown
  protected def onEvent(output: OUTPUT): Boolean
}

abstract class KafkaConsumer[OUTPUT](topic: String, zkConnect: String, groupId: String) extends Consumer[Array[Byte], OUTPUT](topic) {

  protected def createConsumerConnector(zkConnect: String, groupId: String): ConsumerConnector = {
    val props = new Properties()
    props.put("zookeeper.connect", zkConnect)
    props.put("group.id", groupId)
    props.put("zookeeper.session.timeout.ms", "400")
    props.put("zookeeper.sync.time.ms", "200")
    props.put("auto.commit.interval.ms", "1000")
    KConsumer.create(new ConsumerConfig(props))
  }

  protected def createIterator(iter: ConsumerIterator[Array[Byte], Array[Byte]]): Iterator[Array[Byte]] = new Iterator[Array[Byte]]() {
    override def next(): Array[Byte] = iter.next().message()
    override def hasNext: Boolean = iter.hasNext
  }

  protected val consumerConnector = createConsumerConnector(zkConnect, groupId)
  protected val mapStreams = consumerConnector.createMessageStreams(Map(topic -> 1))
  protected val stream = mapStreams.get(topic).get.head
  protected val consumerIterator = stream.iterator()
  override protected val iterator: Iterator[Array[Byte]] = createIterator(consumerIterator)

  override def shutdown {
    consumerConnector.shutdown()
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

  override def onEvent(inputRecord: InputRecord): Boolean = try {
    callback(inputRecord)
  } catch {
    case e: Exception ⇒
      log.severe("Could not run callback on " + inputRecord)
      false
  }
}

