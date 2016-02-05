package mypipe.producer

import java.nio.ByteBuffer

import mypipe.api._
import mypipe.api.event.{ AlterEvent, Serializer, Mutation }
import mypipe.api.producer.Producer
import mypipe.avro.Guid
import mypipe.kafka.KafkaProducer
import com.typesafe.config.Config
import mypipe.avro.schema.{ GenericSchemaRepository }
import org.apache.avro.specific.SpecificRecord
import org.apache.avro.Schema
import org.apache.avro.generic.{ GenericRecord, GenericDatumWriter, GenericData }
import org.apache.avro.io.EncoderFactory
import java.io.ByteArrayOutputStream
import mypipe.kafka.{ PROTO_MAGIC_V0 }
import org.slf4j.LoggerFactory

/** The base class for a Mypipe producer that encodes Mutation instances
 *  as Avro records and publishes them into Kafka.
 *
 *  @param config configuration must have "metadata-brokers"
 */
abstract class KafkaMutationAvroProducer[SchemaId](config: Config)
    extends Producer(config = config) {

  type InputRecord = SpecificRecord
  type OutputType = Array[Byte]

  protected val schemaRepoClient: GenericSchemaRepository[SchemaId, Schema]
  protected val serializer: Serializer[InputRecord, OutputType]

  protected val metadataBrokers = config.getString("metadata-brokers")
  protected val producer = new KafkaProducer[OutputType](metadataBrokers)

  protected val logger = LoggerFactory.getLogger(getClass)
  protected val encoderFactory = EncoderFactory.get()

  /** Builds the Kafka topic using the mutation's database, table name, and
   *  the mutation type (insert, update, delete) concatenating them with "_"
   *  as a delimeter.
   *
   *  @param mutation
   *  @return the topic name
   */
  protected def getKafkaTopic(mutation: Mutation): String

  /** Given a mutation, returns the "subject" that this mutation's
   *  Schema is registered under in the Avro schema repository.
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
      record.put("txid", new GenericData.Fixed(Guid.getClassSchema, uuidBytes.array))
      record.put("txQueryCount", mutation.txQueryCount)
    }
  }

  /** Given a mutation, returns a magic byte that can be associated
   *  with the mutation's type (for example: insert, update, delete).
   *  @param mutation
   *  @return
   */
  protected def magicByte(mutation: Mutation): Byte = Mutation.getMagicByte(mutation)

  /** Given an Avro generic record, schema, and schemaId, serialized
   *  them into an array of bytes.
   *  @param record
   *  @param schema
   *  @param schemaId
   *  @return
   */
  protected def serialize(record: GenericData.Record, schema: Schema, schemaId: SchemaId, mutationType: Byte): Array[Byte] = {
    val encoderFactory = EncoderFactory.get()
    val writer = new GenericDatumWriter[GenericRecord]()
    writer.setSchema(schema)
    val out = new ByteArrayOutputStream()
    out.write(PROTO_MAGIC_V0)
    out.write(mutationType)
    out.write(schemaIdToByteArray(schemaId))
    val enc = encoderFactory.binaryEncoder(out, null)
    writer.write(record, enc)
    enc.flush
    out.toByteArray
  }

  override def queueList(inputList: List[Mutation]): Boolean = {
    inputList.foreach(input ⇒ {
      val schemaTopic = avroSchemaSubject(input)
      val mutationType = magicByte(input)
      val schema = schemaRepoClient.getLatestSchema(schemaTopic).get
      val schemaId = schemaRepoClient.getSchemaId(schemaTopic, schema)
      val records = avroRecord(input, schema)

      records foreach (record ⇒ {
        val bytes = serialize(record, schema, schemaId.get, mutationType)
        producer.send(getKafkaTopic(input), bytes)
      })
    })

    true
  }

  override def queue(input: Mutation): Boolean = {
    try {
      val schemaTopic = avroSchemaSubject(input)
      val mutationType = magicByte(input)
      val schema = schemaRepoClient.getLatestSchema(schemaTopic).get
      val schemaId = schemaRepoClient.getSchemaId(schemaTopic, schema)
      val records = avroRecord(input, schema)

      records foreach (record ⇒ {
        val bytes = serialize(record, schema, schemaId.get, mutationType)
        producer.send(getKafkaTopic(input), bytes)
      })

      true
    } catch {
      case e: Exception ⇒ logger.error(s"failed to queue: ${e.getMessage}\n${e.getStackTraceString}"); false
    }
  }

  override def toString(): String = {
    s"kafka-avro-producer-$metadataBrokers"
  }

}

