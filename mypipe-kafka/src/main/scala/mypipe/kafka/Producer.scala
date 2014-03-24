package mypipe.kafka

import kafka.producer.{ Producer ⇒ KProducer, KeyedMessage, ProducerConfig }

import mypipe.api._

import java.util.Properties
import mypipe.kafka.types._
import org.apache.avro.generic.{ GenericDatumWriter, GenericData, GenericRecord }
import org.apache.avro.Schema.Parser
import mypipe.api.DeleteMutation
import kafka.producer.KeyedMessage
import mypipe.api.UpdateMutation
import mypipe.api.InsertMutation
import java.io.ByteArrayOutputStream
import org.apache.avro.io.EncoderFactory
import java.util.concurrent.LinkedBlockingQueue
import java.util

trait MutationSerializer[OUTPUT] {
  protected def serialize(input: Mutation[_]): OUTPUT
}

abstract class Producer[OUTPUT] extends MutationSerializer[OUTPUT] {

  protected def serialize(input: Mutation[_]): OUTPUT

  def send(message: Mutation[_]) {
    val m = serialize(message)
    queue(getTopic(message), m)
  }

  def send(messages: Array[Mutation[_]]) {
    messages.foreach(m ⇒ queue(getTopic(m), serialize(m)))
  }

  def flush: Boolean
  protected def queue(topic: String, message: OUTPUT)
  protected def getTopic(message: Mutation[_]): String
}

trait AvroVersionedGenericRecordMutationSerializer extends MutationSerializer[Array[Byte]] {

  def serialize(mutation: Mutation[_]): Array[Byte] = {


  }
}

trait AvroGenericRecordMutationSerializer extends MutationSerializer[Array[Byte]] {

  val insertSchemaFile: String
  val updateSchemaFile: String
  val deleteSchemaFile: String

  val parser = new Parser()
  val insertSchema = parser.parse(insertSchemaFile)
  val updateSchema = parser.parse(updateSchemaFile)
  val deleteSchema = parser.parse(deleteSchemaFile)

  val insertWriter = new GenericDatumWriter[GenericRecord](insertSchema)
  val updateWriter = new GenericDatumWriter[GenericRecord](updateSchema)
  val deleteWriter = new GenericDatumWriter[GenericRecord](deleteSchema)

  def serialize(mutation: Mutation[_]): Array[Byte] = {

    mutation match {
      case i: InsertMutation ⇒ serializeInsertMutation(i)
      case u: UpdateMutation ⇒ serializeUpdateMutation(u)
      case d: DeleteMutation ⇒ serializeDeleteMutation(d)
    }
  }

  protected def serializeInsertMutation(i: InsertMutation): Array[Byte] = {
    val record = new GenericData.Record(insertSchema)

    record.put("database", i.table.db)
    record.put("table", i.table.name)

    // we'll probably want to collapse certain data types together here
    // for example, varchar and text might map to the string map
    val cols = i.rows.head.columns.values.groupBy(_.metadata.colType)

    cols.foreach(_ match {

      case (ColumnType.INT24, columns) ⇒
        val map = columns.map(c ⇒ c.metadata.name -> c.value[Int]).toMap
        record.put("integers", map)

      case (ColumnType.VARCHAR, columns) ⇒
        val map = columns.map(c ⇒ c.metadata.name -> c.value[String]).toMap
        record.put("strings", map)

      case _ ⇒ // unsupported
    })

    val out = new ByteArrayOutputStream()
    val encoder = EncoderFactory.get().binaryEncoder(out, null)
    insertWriter.write(record, encoder)
    encoder.flush()
    out.close()
    out.toByteArray()
  }

  protected def serializeUpdateMutation(u: UpdateMutation): Array[Byte] = ???
  protected def serializeDeleteMutation(d: DeleteMutation): Array[Byte] = ???
}

package object types {
  type BinaryKeyedMessage = KeyedMessage[Array[Byte], Array[Byte]]
}

abstract class KafkaProducer(metadataBrokers: String)
    extends Producer[Array[Byte]] {

  val properties = new Properties()
  properties.put("request.required.acks", "1")
  properties.put("metadata.broker.list", metadataBrokers)

  val conf = new ProducerConfig(properties)
  val producer = new KProducer[Array[Byte], Array[Byte]](conf)
  val queue = new LinkedBlockingQueue[BinaryKeyedMessage]()

  override def queue(topic: String, bytes: Array[Byte]) {
    queue.add(new BinaryKeyedMessage(topic, null, bytes))
  }

  override def getTopic(message: Mutation[_]): String = message.table.db + ":" + message.table.name

  override def flush: Boolean = {
    val s = new util.HashSet[BinaryKeyedMessage]
    queue.drainTo(s)
    val a = s.toArray[KeyedMessage[Array[Byte], Array[Byte]]](Array[KeyedMessage[Array[Byte], Array[Byte]]]())
    producer.send(a: _*)
    // TODO: return value properly
    true
  }

}

/** Produces messages into Kafka with binary Avro
 *  encoding via generic records (all tables use the
 *  same Avro bean).
 *
 *  @param metadataBrokers where to find metadata about the Kafka cluster
 */
class KafkaAvroGenericProducer(metadataBrokers: String)
  extends KafkaProducer(metadataBrokers)
  with AvroGenericRecordMutationSerializer {
  override val insertSchemaFile: String = ???
  override val updateSchemaFile: String = ???
  override val deleteSchemaFile: String = ???
}

/** Producers messages into Kafka with binary Avro
 *  encoding via specific records that have to be
 *  registered in an Avro Schema repository.
 *
 *  @param metadataBrokers where to find metadata about the Kafka cluster
 */
class KafkaAvroVersionedSpecificProducer(metadataBrokers: String)
  extends KafkaProducer(metadataBrokers)
  with AvroVersionedSpecificRecordMutationSerializer
