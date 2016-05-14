package mypipe.kafka

import org.slf4j.LoggerFactory

import scala.concurrent._
import ExecutionContext.Implicits.global
import kafka.consumer.{ ConsumerConfig, ConsumerConnector, Consumer ⇒ KConsumer }
import java.util.Properties

import kafka.serializer._
import mypipe.api.event.Mutation
import mypipe.avro._
import mypipe.avro.schema.{ AvroSchemaUtils, GenericSchemaRepository }
import org.apache.avro.Schema

import scala.annotation.tailrec

/** Abstract class that consumes messages for the given topic from
 *  the Kafka cluster running on the provided zkConnect settings. The
 *  groupId is used to control if the consumer resumes from it's saved
 *  offset or not.
 *
 *  @param topic to read messages from
 *  @param zkConnect containing the kafka brokers
 *  @param groupId used to identify the consumer
 */
abstract class KafkaConsumer[T](topic: String, zkConnect: String, groupId: String, valueDecoder: Decoder[T]) {

  protected val log = LoggerFactory.getLogger(getClass.getName)
  protected var future: Future[Unit] = _
  @volatile protected var loop = true
  val iterator = new KafkaIterator(topic, zkConnect, groupId, valueDecoder)

  /** Called every time a new message is pulled from
   *  the Kafka topic.
   *
   *  @param data the data
   *  @return true to continue reading messages, false to stop
   */
  def onEvent(data: T): Boolean

  @tailrec private def consume(): Unit = {
    val message = iterator.next()

    if (message.isDefined) {
      val next = try { onEvent(message.get) }
      catch {
        case e: Exception ⇒ log.error(s"Failed deserializing or processing message. ${e.getMessage} at ${e.getStackTrace.mkString(sys.props("line.separator"))}"); false
      }

      if (next && loop) consume()
    } else {
      // do nothing
    }
  }

  def start: Future[Unit] = {

    future = Future {
      try {
        consume()
        stop()
      } catch {
        case e: Exception ⇒ stop()
      }
    }

    future
  }

  def stop() {
    loop = false
    iterator.stop()
  }
}

case class KafkaGenericAvroDecoder() extends Decoder[Mutation] {

  protected val logger = LoggerFactory.getLogger(getClass.getName)

  protected val schemaRepoClient: GenericSchemaRepository[Short, Schema] = GenericInMemorySchemaRepo
  protected def bytesToSchemaId(bytes: Array[Byte], offset: Int): Short = byteArray2Short(bytes, offset)
  protected def byteArray2Short(data: Array[Byte], offset: Int) = (data(offset) << 8 | data(offset + 1) & 0xff).toShort
  protected def avroSchemaSubjectForMutationByte(byte: Byte): String = AvroSchemaUtils.genericSubject(Mutation.byteToString(byte))
  protected val schemaIdSizeInBytes = 2
  protected val headerLength = PROTO_HEADER_LEN_V0 + schemaIdSizeInBytes

  lazy val insertDeserializer = new AvroVersionedRecordDeserializer[InsertMutation]()
  lazy val updateDeserializer = new AvroVersionedRecordDeserializer[UpdateMutation]()
  lazy val deleteDeserializer = new AvroVersionedRecordDeserializer[DeleteMutation]()

  override def fromBytes(bytes: Array[Byte]): Mutation = {
    val magicByte = bytes(0)

    if (magicByte != PROTO_MAGIC_V0) {
      logger.error(s"We have encountered an unknown magic byte! Magic Byte: $magicByte")
      null
    } else {
      val mutationType = bytes(1)
      val schemaId = bytesToSchemaId(bytes, PROTO_HEADER_LEN_V0)

      mutationType match {
        case Mutation.InsertByte ⇒ schemaRepoClient
          .getSchema(avroSchemaSubjectForMutationByte(Mutation.InsertByte), schemaId)
          .flatMap(insertDeserializer.deserialize(_, bytes, headerLength))
          .getOrElse(null)
          .asInstanceOf[Mutation]

        case Mutation.UpdateByte ⇒ schemaRepoClient
          .getSchema(avroSchemaSubjectForMutationByte(Mutation.UpdateByte), schemaId)
          .flatMap(updateDeserializer.deserialize(_, bytes, headerLength))
          .getOrElse(null)
          .asInstanceOf[Mutation]

        case Mutation.DeleteByte ⇒ schemaRepoClient
          .getSchema(avroSchemaSubjectForMutationByte(Mutation.DeleteByte), schemaId)
          .flatMap(deleteDeserializer.deserialize(_, bytes, headerLength))
          .getOrElse(null)
          .asInstanceOf[Mutation]
      }
    }
  }
}

class KafkaIterator[T](topic: String, zkConnect: String, groupId: String, valueDecoder: Decoder[T]) {
  protected val consumerConnector = createConsumerConnector(zkConnect, groupId)
  protected val mapStreams = consumerConnector.createMessageStreams(Map(topic -> 1), keyDecoder = new DefaultDecoder(), valueDecoder = valueDecoder)
  protected val stream = mapStreams.get(topic).get.head
  protected val consumerIterator = stream.iterator()

  protected def createConsumerConnector(zkConnect: String, groupId: String): ConsumerConnector = {
    val props = new Properties()
    props.put("zookeeper.connect", zkConnect)
    props.put("group.id", groupId)
    props.put("zookeeper.session.timeout.ms", "400")
    props.put("zookeeper.sync.time.ms", "200")
    props.put("auto.commit.interval.ms", "1000")
    KConsumer.create(new ConsumerConfig(props))
  }

  // TODO: this can't be a mutation, it might have to be a specificrecord
  def next(): Option[T] = Option(consumerIterator.next()).map(_.message())
  def stop(): Unit = consumerConnector.shutdown()
}
