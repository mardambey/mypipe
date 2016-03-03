package mypipe.producer

import java.nio.ByteBuffer

import mypipe.api._
import mypipe.api.event.{ AlterEvent, Serializer, Mutation }
import mypipe.api.producer.Producer
import mypipe.sqs.SQSProducer
import com.typesafe.config.Config
import mypipe.avro.schema.{ GenericSchemaRepository }
import org.apache.avro.specific.SpecificRecord
import org.apache.avro.Schema
import org.apache.avro.generic.{ GenericRecord, GenericDatumWriter, GenericData }
import org.apache.avro.io.EncoderFactory
import java.io.ByteArrayOutputStream
import org.slf4j.LoggerFactory

/** The base class for a Mypipe producer that encodes Mutation instances
 *  as Avro records and publishes them into SQS.
 *
 */
abstract class SQSMutationAvroProducer[SchemaId](config: Config)
    extends Producer(config = config) {

  type InputRecord = SpecificRecord
  type OutputType = Array[Byte]

  protected val schemaRepoClient: GenericSchemaRepository[SchemaId, Schema]
  protected val serializer: Serializer[InputRecord, OutputType]

  protected val sqsQueue = config.getString("sqs-queue")
  protected val producer = new SQSProducer(sqsQueue)

  protected val logger = LoggerFactory.getLogger(getClass)
  protected val encoderFactory = EncoderFactory.get()

  /** Builds the SQS topic using the mutation's database, table name, and
   *  the mutation type (insert, update, delete) concatenating them with "_"
   *  as a delimeter.
   *
   *  @param mutation
   *  @return the topic name
   */
  protected def getSQSTopic(mutation: Mutation): String

  /** Given a mutation, returns the "subject" that this mutation's
   *  Schema is registered under in the Avro schema repository.
   *
   *  @param mutation
   *  @return
   */
  protected def avroSchemaSubject(mutation: Mutation): String

  /** Given a schema ID of type SchemaId, converts it to a byte array.
   *
   *  @param schemaId
   *  @return
   */
  protected def schemaIdToByteArray(schemaId: SchemaId): Array[Byte]

  /** Given a Mutation, this method must convert it into a(n) Avro record(s)
   *  for the given Avro schema.
   *
   *  @param mutation
   *  @param schema
   *  @return the Avro generic record(s)
   */
  protected def avroRecord(mutation: Mutation, schema: Schema): List[GenericData.Record]

  override def flush(): Boolean = {
    try {
      producer.flush
      true
    } catch {
      case e: Exception ⇒ {
        logger.error(s"Could not flush producer queue: ${e.getMessage} -> ${e.getStackTraceString}")
        false
      }
    }
  }

  /** Adds a header into the given Record based on the Mutation's
   *  database, table, and tableId.
   *
   *  @param record
   *  @param mutation
   */
  protected def header(record: GenericData.Record, mutation: Mutation) {
    record.put("database", mutation.table.db)
    record.put("table", mutation.table.name)
    record.put("tableId", mutation.table.id)

    // TODO: avoid null check
    if (mutation.txid != null && record.getSchema().getField("txid") != null) {
      val uuidBytes = ByteBuffer.wrap(new Array[Byte](16))
      uuidBytes.putLong(mutation.txid.getMostSignificantBits)
      uuidBytes.putLong(mutation.txid.getLeastSignificantBits)

      val uuidString = uuidBytes.array.map { b ⇒ String.format("%02x", new java.lang.Integer(b & 0xff)) }.mkString.replaceFirst("(\\p{XDigit}{8})(\\p{XDigit}{4})(\\p{XDigit}{4})(\\p{XDigit}{4})(\\p{XDigit}+)", "$1-$2-$3-$4-$5")

      record.put("txid", uuidString)
      record.put("txQueryCount", mutation.txQueryCount)
    }
  }

  /** Given a mutation, returns a string (for example: insert, update, delete).
   *
   *  @param mutation
   *  @return
   */
  protected def mutationTypeString(mutation: Mutation): String = Mutation.typeAsString(mutation)

  /** Given an Avro generic record, schema, and schemaId, serialized
   *  them into an array of bytes.
   *
   *  @param record
   *  @param schema
   *  @param schemaId
   *  @return
   */
  protected def serialize(record: GenericData.Record, schema: Schema, schemaId: SchemaId, mutationType: String): String = {
    val encoderFactory = EncoderFactory.get()
    val writer = new GenericDatumWriter[GenericRecord]()
    writer.setSchema(schema)
    val out = new ByteArrayOutputStream()

    val enc = encoderFactory.jsonEncoder(schema, out)
    writer.write(record, enc)
    enc.flush

    // Dirty hack to work around Avro schema objects' apparent lack of support for schema modification
    val preMutationJson = out.toString
    preMutationJson.substring(0, preMutationJson.length - 1) + ",\"mutation\":\"" + mutationType + "\"}"
  }

  override def queueList(inputList: List[Mutation]): Boolean = {
    inputList.foreach(input ⇒ {
      val schemaTopic = avroSchemaSubject(input)
      val mutationType = mutationTypeString(input)
      val schema = schemaRepoClient.getLatestSchema(schemaTopic).get
      val schemaId = schemaRepoClient.getSchemaId(schemaTopic, schema)
      val records = avroRecord(input, schema)

      records foreach (record ⇒ {
        val jsonString = serialize(record, schema, schemaId.get, mutationType)
        producer.send(jsonString)
      })
    })

    true
  }

  override def queue(input: Mutation): Boolean = {
    try {
      val schemaTopic = avroSchemaSubject(input)
      val mutationType = mutationTypeString(input)
      val schema = schemaRepoClient.getLatestSchema(schemaTopic).get
      val schemaId = schemaRepoClient.getSchemaId(schemaTopic, schema)
      val records = avroRecord(input, schema)

      records foreach (record ⇒ {
        val jsonString = serialize(record, schema, schemaId.get, mutationType)
        producer.send(jsonString)
      })

      true
    } catch {
      case e: Exception ⇒ logger.error(s"failed to queue: ${e.getMessage}\n${e.getStackTraceString}"); false
    }
  }

  override def toString(): String = {
    s"sqs-avro-producer-$sqsQueue"
  }

}

