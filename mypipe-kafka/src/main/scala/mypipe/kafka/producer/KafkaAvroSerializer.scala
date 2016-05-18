package mypipe.kafka.producer

import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer
import java.util

import mypipe.api.data.Row
import mypipe.api.event.{ Mutation, SingleValuedMutation, UpdateMutation }
import mypipe.avro.Guid
import mypipe.avro.schema.GenericSchemaRepository
import org.apache.avro.Schema
import org.apache.avro.generic.{ GenericData, GenericDatumWriter, GenericRecord }
import org.apache.avro.io.EncoderFactory
import org.apache.kafka.common.serialization.Serializer
import org.slf4j.LoggerFactory
import mypipe.kafka.PROTO_MAGIC_V0

abstract class KafkaAvroSerializer() extends Serializer[(Mutation, Either[Row, (Row, Row)])] {

  protected val logger = LoggerFactory.getLogger(getClass)

  protected var schemaRepoClient: GenericSchemaRepository[Short, Schema] = _

  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {
    val schemaRepoClientClassName = configs.get("schema-repo-client")

    schemaRepoClient = Class.forName(schemaRepoClientClassName + "$")
      .getField("MODULE$").get(null)
      .asInstanceOf[GenericSchemaRepository[Short, Schema]]
  }

  override def serialize(topic: String, mutationAndRecord: (Mutation, Either[Row, (Row, Row)])): Array[Byte] = {
    try {
      val mutation = mutationAndRecord._1
      val rowOrTupleRows = mutationAndRecord._2

      val schemaTopic = avroSchemaSubject(mutation)
      val mutationType = magicByte(mutation)
      val schema = schemaRepoClient.getLatestSchema(schemaTopic)

      if (schema.isDefined) {

        val schemaId = schemaRepoClient.getSchemaId(schemaTopic, schema.get)
        val record = avroRecord(mutation, rowOrTupleRows, schema.get)

        serialize(record, schema.get, schemaId.get, mutationType)

      } else {
        logger.error(s"Could not find schema for schemaTopic: $schemaTopic and mutation: $mutation")
        Array.empty
      }
    } catch {
      case e: Exception ⇒
        logger.error(s"failed to queue: ${e.getMessage}\n${e.getStackTraceString}")
        Array.empty
    }
  }

  override def close(): Unit = {

  }

  /** Given a mutation, returns the "subject" that this mutation's
   *  Schema is registered under in the Avro schema repository.
   *
   *  @param mutation mutation
   *  @return
   */
  protected def avroSchemaSubject(mutation: Mutation): String

  /** Builds the Kafka topic using the mutation's database, table name, and
   *  the mutation type (insert, update, delete) concatenating them with "_"
   *  as a delimeter.
   *
   *  @param mutation mutation
   *  @return the topic name
   */
  protected def getKafkaTopic(mutation: Mutation): String

  /** Given a schema ID of type SchemaId, converts it to a byte array.
   *
   *  @param schemaId schema id
   *  @return
   */
  protected def schemaIdToByteArray(schemaId: Short): Array[Byte]

  /** Given a mutation, returns a magic byte that can be associated
   *  with the mutation's type (for example: insert, update, delete).
   *
   *  @param mutation mutation
   *  @return magic byte
   */
  protected def magicByte(mutation: Mutation): Byte = Mutation.getMagicByte(mutation)

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

  protected def body(record: GenericData.Record, row: Row, schema: Schema)(implicit keyOp: String ⇒ String = s ⇒ s)

  protected def insertOrDeleteMutationToAvro(mutation: SingleValuedMutation, row: Row, schema: Schema): GenericData.Record = {
    val record = new GenericData.Record(schema)
    addHeader(record, mutation)
    body(record, row, schema)
    record
  }

  protected def updateMutationToAvro(mutation: UpdateMutation, row: (Row, Row), schema: Schema): GenericData.Record = {
    val record = new GenericData.Record(schema)
    addHeader(record, mutation)
    body(record, row._1, schema) { s ⇒ "old_" + s }
    body(record, row._2, schema) { s ⇒ "new_" + s }
    record
  }

  /** Given a Mutation, this method must convert it into a(n) Avro record(s)
   *  for the given Avro schema.
   *
   *  @param mutation mutation
   *  @param schema Avro schema
   *  @return the Avro generic record(s)
   */
  private def avroRecord(mutation: Mutation, row: Either[Row, (Row, Row)], schema: Schema): GenericData.Record = {

    Mutation.getMagicByte(mutation) match {
      case Mutation.InsertByte ⇒ insertOrDeleteMutationToAvro(mutation.asInstanceOf[SingleValuedMutation], row.left.get, schema)
      case Mutation.DeleteByte ⇒ insertOrDeleteMutationToAvro(mutation.asInstanceOf[SingleValuedMutation], row.left.get, schema)
      case Mutation.UpdateByte ⇒ updateMutationToAvro(mutation.asInstanceOf[UpdateMutation], row.right.get, schema)
      case _ ⇒
        logger.error(s"Unexpected mutation type ${mutation.getClass} encountered; retuning empty Avro GenericData.Record(schema=$schema")
        new GenericData.Record(schema)
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
  private def serialize(record: GenericData.Record, schema: Schema, schemaId: Short, mutationType: Byte): Array[Byte] = {
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

