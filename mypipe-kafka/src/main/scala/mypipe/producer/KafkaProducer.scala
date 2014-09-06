package mypipe.producer

import mypipe.api._
import mypipe.kafka.KafkaProducer
import com.typesafe.config.Config
import mypipe.avro.schema.{ GenericSchemaRepository, SchemaRepo }
import org.apache.avro.specific.SpecificRecord
import mypipe.avro.{ AvroVersionedRecordSerializer, GenericInMemorySchemaRepo }
import org.apache.avro.Schema
import java.lang.{ Long ⇒ JLong }
import java.util.{ HashMap ⇒ JMap }
import org.apache.avro.generic.{ GenericRecord, GenericDatumWriter, GenericData }
import org.apache.avro.io.EncoderFactory
import java.io.ByteArrayOutputStream
import mypipe.kafka.{ PROTO_MAGIC_V0 }
import org.slf4j.LoggerFactory

case class MutationStringAndByte(string: String, byte: Byte)

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
  protected def getKafkaTopic(mutation: Mutation[_]): String =
    s"${mutation.table.db}_${mutation.table.name}"

  /** Given a Mutation, this method must compute the "topic"
   *  associated with this mutation in the Avro schema repository
   *  as well as the byte that represents it when it is serialized
   *  into a byte array. The byte is one of:
   *
   *  0x0 -> insert
   *  0x1 -> update
   *  0x2 -> delete
   *
   *  @param mutation
   *  @return the "topic"
   */
  protected def mutationToAvroSchemaTopicAndByte(mutation: Mutation[_]): MutationStringAndByte

  /** Given a schema ID of type SchemaId, converts it to a byte array.
   *
   *  @param schemaId
   *  @return
   */
  protected def schemaIdToByteArray(schemaId: SchemaId): Array[Byte]

  /** Given a Mutation, this method must convert it into an Avro record
   *  for the given Avro schema.
   *
   *  @param mutation
   *  @param schema
   *  @return the Avro generic record
   */
  protected def mutationToAvro(mutation: Mutation[_], schema: Schema): GenericData.Record

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

  protected def mutationToStringAndByte(mutation: Mutation[_]): MutationStringAndByte = {
    MutationStringAndByte(Mutation.typeAsString(mutation), Mutation.getMagicByte(mutation))
  }

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

  override def queueList(inputList: List[Mutation[_]]): Boolean = {
    inputList.foreach(input ⇒ {
      val MutationStringAndByte(schemaTopic, mutationType) = mutationToAvroSchemaTopicAndByte(input)
      val schema = schemaRepoClient.getLatestSchema(schemaTopic).get
      val schemaId = schemaRepoClient.getSchemaId(schemaTopic, schema)
      val record = mutationToAvro(input, schema)
      val bytes = serialize(record, schema, schemaId.get, mutationType)
      producer.send(getKafkaTopic(input), bytes)
    })

    true
  }

  override def queue(input: Mutation[_]): Boolean = {
    val MutationStringAndByte(schemaTopic, mutationType) = mutationToAvroSchemaTopicAndByte(input)
    val schema = schemaRepoClient.getLatestSchema(schemaTopic).get
    val schemaId = schemaRepoClient.getSchemaId(schemaTopic, schema)
    val record = mutationToAvro(input, schema)
    val bytes = serialize(record, schema, schemaId.get, mutationType)

    producer.send(getKafkaTopic(input), bytes)
    true
  }

  override def toString(): String = {
    s"kafka-avro-producer-$metadataBrokers"
  }

}

/** An implementation of the base KafkaMutationAvroProducer class that uses a
 *  GenericInMemorySchemaRepo in order to encode mutations as Avro beans.
 *  Three beans are encoded: mypipe.avro.InsertMutation, UpdateMutation, and
 *  DeleteMutation. The Kafka topic names are calculated as:
 *  dbName_tableName_(insert|update|delete)
 *
 *  @param config configuration must have "metadata-brokers"
 */
class KafkaMutationGenericAvroProducer(config: Config)
    extends KafkaMutationAvroProducer[Short](config) {

  override protected val schemaRepoClient = GenericInMemorySchemaRepo
  override protected val serializer = new AvroVersionedRecordSerializer[InputRecord](schemaRepoClient)

  override protected def mutationToAvroSchemaTopicAndByte(mutation: Mutation[_]): MutationStringAndByte =
    mutationToStringAndByte(mutation)

  /** Given a short, returns a byte array.
   *  @param s
   *  @return
   */
  override protected def schemaIdToByteArray(s: Short) = Array[Byte](((s & 0xFF00) >> 8).toByte, (s & 0x00FF).toByte)

  override protected def mutationToAvro(mutation: Mutation[_], schema: Schema): GenericData.Record = {

    Mutation.getMagicByte(mutation) match {

      case Mutation.InsertByte ⇒ {
        val (integers, strings, longs) = columnsToMaps(mutation.asInstanceOf[InsertMutation].rows.head.columns)
        val record = new GenericData.Record(schema)
        header(record, mutation)
        body(record, mutation, integers, strings, longs)
        record
      }

      case Mutation.DeleteByte ⇒ {
        val (integers, strings, longs) = columnsToMaps(mutation.asInstanceOf[DeleteMutation].rows.head.columns)
        val record = new GenericData.Record(schema)
        header(record, mutation)
        body(record, mutation, integers, strings, longs)

        record
      }

      case Mutation.UpdateByte ⇒ {
        val (integersOld, stringsOld, longsOld) = columnsToMaps(mutation.asInstanceOf[UpdateMutation].rows.head._1.columns)
        val (integersNew, stringsNew, longsNew) = columnsToMaps(mutation.asInstanceOf[UpdateMutation].rows.head._2.columns)
        val record = new GenericData.Record(schema)
        header(record, mutation)
        body(record, mutation, integersOld, stringsOld, longsOld) { s ⇒ "old_" + s }
        body(record, mutation, integersNew, stringsNew, longsNew) { s ⇒ "new_" + s }

        record
      }

      case _ ⇒ {
        logger.error(s"Unexpected mutation type ${mutation.getClass} encountered; retuning empty Avro GenericData.Record(schema=$schema")
        new GenericData.Record(schema)
      }
    }
  }

  protected def body(record: GenericData.Record,
                     mutation: Mutation[_],
                     integers: JMap[CharSequence, Integer],
                     strings: JMap[CharSequence, CharSequence],
                     longs: JMap[CharSequence, JLong])(implicit keyOp: String ⇒ String = s ⇒ s) {
    record.put(keyOp("integers"), integers)
    record.put(keyOp("strings"), strings)
    record.put(keyOp("longs"), longs)
  }

  protected def header(record: GenericData.Record, mutation: Mutation[_]) {
    record.put("database", mutation.table.db)
    record.put("table", mutation.table.name)
    record.put("tableId", mutation.table.id)
  }

  protected def columnsToMaps(columns: Map[String, Column]): (JMap[CharSequence, Integer], JMap[CharSequence, CharSequence], JMap[CharSequence, JLong]) = {

    val cols = columns.values.groupBy(_.metadata.colType)

    // ugliness follows... we'll clean it up some day.
    val integers = new java.util.HashMap[CharSequence, Integer]()
    val strings = new java.util.HashMap[CharSequence, CharSequence]()
    val longs = new java.util.HashMap[CharSequence, java.lang.Long]()

    cols.foreach(_ match {

      case (ColumnType.INT24, columns) ⇒
        columns.foreach(c ⇒ {
          val v = c.valueOption[Int]
          if (v.isDefined) integers.put(c.metadata.name, v.get)
        })

      case (ColumnType.VARCHAR, columns) ⇒
        columns.foreach(c ⇒ {
          val v = c.valueOption[String]
          if (v.isDefined) strings.put(c.metadata.name, v.get)
        })

      case (ColumnType.LONG, columns) ⇒
        columns.foreach(c ⇒ {
          // this damn thing can come in as an Integer or Long
          val v = c.value match {
            case i: java.lang.Integer ⇒ new java.lang.Long(i.toLong)
            case l: java.lang.Long    ⇒ l
            case null                 ⇒ null
          }

          if (v != null) longs.put(c.metadata.name, v)
        })

      case _ ⇒ // unsupported
    })

    (integers, strings, longs)
  }
}

class KafkaMutationSpecificAvroProducer(config: Config)
    extends KafkaMutationAvroProducer[Short](config) {

  private val schemaRepoClientClassName = config.getString("schema-repo-client")

  override protected val serializer = new AvroVersionedRecordSerializer[InputRecord](schemaRepoClient)
  override protected val schemaRepoClient = Class.forName(schemaRepoClientClassName)
    .newInstance()
    .asInstanceOf[GenericSchemaRepository[Short, Schema]]

  override protected def schemaIdToByteArray(s: Short) = Array[Byte](((s & 0xFF00) >> 8).toByte, (s & 0x00FF).toByte)

  override protected def mutationToAvroSchemaTopicAndByte(mutation: Mutation[_]): MutationStringAndByte = {
    val strAndByte = mutationToStringAndByte(mutation)
    MutationStringAndByte(s"${mutation.table.db}_${mutation.table.name}_${strAndByte.string}", strAndByte.byte)
  }

  override protected def mutationToAvro(mutation: Mutation[_], schema: Schema): GenericData.Record = {

    Mutation.getMagicByte(mutation) match {
      case Mutation.InsertByte ⇒ insertMutationToAvro(mutation.asInstanceOf[InsertMutation], schema)
      case Mutation.UpdateByte ⇒ updateMutationToAvro(mutation.asInstanceOf[UpdateMutation], schema)
      case Mutation.DeleteByte ⇒ deleteMutationToAvro(mutation.asInstanceOf[DeleteMutation], schema)
      case _ ⇒ {
        logger.error(s"Unexpected mutation type ${mutation.getClass} encountered; retuning empty Avro GenericData.Record(schema=$schema")
        new GenericData.Record(schema)
      }
    }
  }

  protected def insertMutationToAvro(mutation: InsertMutation, schema: Schema): GenericData.Record = {

    val record = new GenericData.Record(schema)

    mutation.rows.head.columns.foreach(col ⇒ {

      val field = Option(schema.getField(col._1))

      if (field.isDefined) {
        record.put(field.get.name(), col._2.value)
      }
    })

    record
  }

  protected def updateMutationToAvro(mutation: UpdateMutation, schema: Schema): GenericData.Record = ???
  protected def deleteMutationToAvro(mutation: DeleteMutation, schema: Schema): GenericData.Record = ???
}
