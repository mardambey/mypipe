package mypipe.kafka

import mypipe.api.{ Mutation, MutationDeserializer }
import kafka.consumer.{ ConsumerIterator, ConsumerConnector, Consumer, ConsumerConfig }
import java.util.Properties
import java.util.logging.Logger
import org.apache.avro.generic.{ GenericData, GenericRecord, GenericDatumReader }
import org.apache.avro.Schema
import org.apache.avro.io.DecoderFactory
import scala.concurrent._
import ExecutionContext.Implicits.global

abstract class Consumer[INPUT](topic: String) extends MutationDeserializer[INPUT] {

  val log = Logger.getLogger(getClass.getName)
  val iterator: Iterator[INPUT]
  var future: Future[Unit] = _
  @volatile var loop = true

  def start {

    future = Future {
      iterator.takeWhile(message ⇒ {
        val next = try { onEvent(deserialize(message)) } catch {
          case e: Exception ⇒ log.severe("Failed deserializing or processing message."); false
        }

        next && loop
      })
    }

    shutdown
  }

  def stop {
    loop = false
  }

  def onEvent(mutation: Mutation[_]): Boolean
  def shutdown

}

trait AvroRecordMutationDeserializer extends MutationDeserializer[Array[Byte]] {

  def deserialize(bytes: Array[Byte]): Mutation[_] = {
    val magic = bytes(0)
    val schemaId = getSchemaIdFromBytes(bytes, 1)
    val schema = getSchemaById(schemaId)
    val decoder = DecoderFactory.get().binaryDecoder(Array[Byte](), null)
    val reader = new GenericDatumReader[GenericRecord](schema)
    val record = new GenericData.Record(schema)
    reader.setSchema(schema)
    reader.read(record, DecoderFactory.get().binaryDecoder(bytes, 3, bytes.length - 3, decoder))
    genericRecordToMutation(record)
  }

  def getSchemaIdFromBytes(bytes: Array[Byte], offset: Int): Short = byteArray2Short(bytes, 1)
  protected def byteArray2Short(data: Array[Byte], offset: Int) = (((data(offset) << 8)) | ((data(offset + 1) & 0xff))).toShort

  def genericRecordToMutation(record: GenericData.Record): Mutation[_]
  def getSchemaById(schemaId: Short): Schema
}

trait AvroGenericRecordMutationDeserializer extends AvroRecordMutationDeserializer {

  // schema ids, used to distinguish which Avro schema to use when decoding
  val INSERT: Short = -1
  val UPDATE: Short = -2
  val DELETE: Short = -3

  // Avro schemas to be used when serializing mutations
  val insertSchemaFile: String = "/InsertMutation.avsc"
  val updateSchemaFile: String = "/UpdateMutation.avsc"
  val deleteSchemaFile: String = "/DeleteMutation.avsc"

  val insertSchema = try { Schema.parse(getClass.getResourceAsStream(insertSchemaFile)) } catch { case e: Exception ⇒ println("Failed on insert: " + e.getMessage); null }
  val updateSchema = try { Schema.parse(getClass.getResourceAsStream(updateSchemaFile)) } catch { case e: Exception ⇒ println("Failed on update: " + e.getMessage); null }
  val deleteSchema = try { Schema.parse(getClass.getResourceAsStream(deleteSchemaFile)) } catch { case e: Exception ⇒ println("Failed on delete: " + e.getMessage); null }

  def getSchemaById(schemaId: Short): Schema = schemaId match {
    case INSERT ⇒ insertSchema
    case UPDATE ⇒ updateSchema
    case DELETE ⇒ deleteSchema
  }

  def genericRecordToMutation(record: GenericData.Record): Mutation[_] = {
    record.getSchema match {
      case insertSchema ⇒
      case updateSchema ⇒
      case deleteSchema ⇒
    }

    ???
  }
}

trait AvroVersionedSpecificRecordMutationDeserializer extends AvroRecordMutationDeserializer

abstract class KafkaConsumer(topic: String, zkConnect: String, groupId: String)
    extends Consumer[Array[Byte]](topic) {

  def createConsumerConnector(zkConnect: String, groupId: String): ConsumerConnector = {
    val props = new Properties()
    props.put("zookeeper.connect", zkConnect)
    props.put("group.id", groupId)
    props.put("zookeeper.session.timeout.ms", "400")
    props.put("zookeeper.sync.time.ms", "200")
    props.put("auto.commit.interval.ms", "1000")
    Consumer.create(new ConsumerConfig(props))
  }

  def createIterator(iter: ConsumerIterator[Array[Byte], Array[Byte]]): Iterator[Array[Byte]] = new Iterator[Array[Byte]]() {
    override def next(): Array[Byte] = iter.next().message()
    override def hasNext: Boolean = iter.hasNext
  }

  val consumerConnector = createConsumerConnector(zkConnect, groupId)
  val mapStreams = consumerConnector.createMessageStreams(Map(topic -> 1))
  val stream = mapStreams.get(topic).get.head
  val consumerIterator = stream.iterator()
  val iterator: Iterator[Array[Byte]] = createIterator(consumerIterator)

  def shutdown {
    consumerConnector.shutdown()
  }

  def onEvent(mutation: Mutation[_]): Boolean = ???
}

abstract class KafkaAvroGenericConsumer(topic: String, zkConnect: String, groupId: String)
  extends KafkaConsumer(topic, zkConnect, groupId)
  with AvroGenericRecordMutationDeserializer

abstract class KafkaAvroVersionedSpecifcConsumer(topic: String, zkConnect: String, groupId: String)
  extends KafkaConsumer(topic, zkConnect, groupId)
  with AvroVersionedSpecificRecordMutationDeserializer

