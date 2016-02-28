package mypipe.producer

import java.nio.ByteBuffer

import mypipe.api.data.Row
import mypipe.api.event.{ UpdateMutation, SingleValuedMutation, Serializer, Mutation }
import mypipe.api.producer.Producer
import mypipe.avro.Guid
import mypipe.kafka.KafkaProducer
import com.typesafe.config.Config
import mypipe.avro.schema.GenericSchemaRepository
import org.apache.avro.specific.SpecificRecord
import org.apache.avro.Schema
import org.apache.avro.generic.{ GenericRecord, GenericDatumWriter, GenericData }
import org.apache.avro.io.EncoderFactory
import java.io.ByteArrayOutputStream
import mypipe.kafka.PROTO_MAGIC_V0
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
   *  @param mutation mutation
   *  @return the topic name
   */
  protected def getKafkaTopic(mutation: Mutation): String

  /** Given a mutation, returns the "subject" that this mutation's
   *  Schema is registered under in the Avro schema repository.
   *
   *  @param mutation mutation
   *  @return
   */
  protected def avroSchemaSubject(mutation: Mutation): String

  /** Given a schema ID of type SchemaId, converts it to a byte array.
   *
   *  @param schemaId schema id
   *  @return
   */
  protected def schemaIdToByteArray(schemaId: SchemaId): Array[Byte]

  protected def body(record: GenericData.Record, row: Row, schema: Schema)(implicit keyOp: String ⇒ String = s ⇒ s)

  protected def insertOrDeleteMutationToAvro(mutation: SingleValuedMutation, schema: Schema): List[GenericData.Record] = {
    mutation.rows.map(row ⇒ {
      val record = new GenericData.Record(schema)
      addHeader(record, mutation)
      body(record, row, schema)
      record
    })
  }

  protected def updateMutationToAvro(mutation: UpdateMutation, schema: Schema): List[GenericData.Record] = {
    mutation.rows.map(row ⇒ {
      val record = new GenericData.Record(schema)
      addHeader(record, mutation)
      body(record, row._1, schema) { s ⇒ "old_" + s }
      body(record, row._2, schema) { s ⇒ "new_" + s }
      record
    })
  }

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
      val schemaTopic = avroSchemaSubject(input)
      val mutationType = magicByte(input)
      val schema = schemaRepoClient.getLatestSchema(schemaTopic)

      if (schema.isDefined) {

        val schemaId = schemaRepoClient.getSchemaId(schemaTopic, schema.get)
        val records = avroRecord(input, schema.get)

        if (input.isInstanceOf[SingleValuedMutation]) {
          val mut = input.asInstanceOf[SingleValuedMutation]
          (records zip mut.rows) foreach {
            case (record, row) ⇒
              val bytes = serialize(record, schema.get, schemaId.get, mutationType)
              val pKeyStr = SingleValuedMutation.primaryKeyAsString(mut, row)
              producer.queue(getKafkaTopic(input), pKeyStr.getOrElse("").getBytes("utf-8"), bytes)
          }
        } else {
          records foreach { record ⇒
            val bytes = serialize(record, schema.get, schemaId.get, mutationType)
            producer.queue(getKafkaTopic(input), bytes)
          }
        }

        true
      } else {
        logger.error(s"Could not find schema for schemaTopic: $schemaTopic and mutation: $input")
        false
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

  /** Adds a header into the given Record based on the Mutation's
   *  database, table, and tableId.
   *
   *  @param record Avro generic record
   *  @param mutation mutation
   */
  protected def addHeader(record: GenericData.Record, mutation: Mutation) {
    record.put("database", mutation.table.db)
    record.put("table", mutation.table.name)
    record.put("tableId", mutation.table.id)

    // TODO: avoid null check
    if (mutation.txid != null && record.getSchema.getField("txid") != null) {
      val uuidBytes = ByteBuffer.wrap(new Array[Byte](16))
      uuidBytes.putLong(mutation.txid.getMostSignificantBits)
      uuidBytes.putLong(mutation.txid.getLeastSignificantBits)
      record.put("txid", new GenericData.Fixed(Guid.getClassSchema, uuidBytes.array))
    }
  }

  /** Given a mutation, returns a magic byte that can be associated
   *  with the mutation's type (for example: insert, update, delete).
   *
   *  @param mutation mutation
   *  @return magic byte
   */
  protected def magicByte(mutation: Mutation): Byte = Mutation.getMagicByte(mutation)

  /** Given a Mutation, this method must convert it into a(n) Avro record(s)
   *  for the given Avro schema.
   *
   *  @param mutation mutation
   *  @param schema Avro schema
   *  @return the Avro generic record(s)
   */
  private def avroRecord(mutation: Mutation, schema: Schema): List[GenericData.Record] = {

    Mutation.getMagicByte(mutation) match {
      case Mutation.InsertByte ⇒ insertOrDeleteMutationToAvro(mutation.asInstanceOf[SingleValuedMutation], schema)
      case Mutation.DeleteByte ⇒ insertOrDeleteMutationToAvro(mutation.asInstanceOf[SingleValuedMutation], schema)
      case Mutation.UpdateByte ⇒ updateMutationToAvro(mutation.asInstanceOf[UpdateMutation], schema)
      case _ ⇒
        logger.error(s"Unexpected mutation type ${mutation.getClass} encountered; retuning empty Avro GenericData.Record(schema=$schema")
        List(new GenericData.Record(schema))
    }
  }

  /** Given an Avro generic record, schema, and schemaId, serialize
   *  them into an array of bytes.
   *
   *  @param record Avro generic record
   *  @param schema Avro schema
   *  @param schemaId schema id
   *  @return
   */
  private def serialize(record: GenericData.Record, schema: Schema, schemaId: SchemaId, mutationType: Byte): Array[Byte] = {
    val encoderFactory = EncoderFactory.get()
    val writer = new GenericDatumWriter[GenericRecord]()
    writer.setSchema(schema)
    val out = new ByteArrayOutputStream()
    out.write(PROTO_MAGIC_V0)
    out.write(mutationType)
    out.write(schemaIdToByteArray(schemaId))
    val enc = encoderFactory.binaryEncoder(out, null)
    writer.write(record, enc)
    enc.flush()
    out.toByteArray
  }
}

