package mypipe.avro

import mypipe.api._
import org.apache.avro.io.DecoderFactory
import org.apache.avro.generic.{ GenericData, GenericRecord, GenericDatumReader }
import org.apache.avro.Schema
import mypipe.api.Column
import scala.collection.JavaConverters._
import java.util.{ Map ⇒ JMap }
import java.lang.{ Long ⇒ JLong }
import mypipe.avro.schema.SchemaRepository

/**
 * Deserializes Avro data into Mutation instances.
 *
 * Requires that the user of the trait provides an Avro schema repository implementation,
 * a way to map schema IDs (given a mutation type) to topic names, and a way to read columns
 * from an Avro record mapping them to Column objects.
 */
trait AvroRecordMutationDeserializer extends MutationDeserializer[Array[Byte]] with NameMappers {

  protected val MAGIC: Byte = 0
  protected val HEADER_SIZE = 3

  /**
   * Used to get schemas for a given schema ID.
   */
  protected val schemaRepoClient: SchemaRepository[Short, Schema]

  /**
   * Computes the topic name for the given schema ID and mutation class.
   *
   * @param schemaId
   * @param mutationClass
   */
  protected def topicFor(schemaId: Short, mutationClass: Class[Mutation[_]]): String

  /**
   * Reads columns given an Avro record and applies the a name mapper when fetching column names.
   *
   * @param record to read columns from.
   * @param nameMapper to apply on the column names before
   * @return
   */
  protected def readColumns(record: GenericData.Record, nameMapper: NameMapper = IdentityNameMapper): Seq[Column]

  /**
   * Checks if the record represents an insert, update, or delete.
   * Once records are checked they are deserialized and the resulting Mutation is returned.
   *
   * @param bytes
   * @return a Mutation (InsertMutation, UpdateMutation, DeleteMutation) instance.
   */
  override protected def deserialize[MUTATION](bytes: Array[Byte])(implicit m: reflect.ClassTag[MUTATION]): MUTATION = {
    val (magic, schemaId) = readHeader(bytes).get
    val record = readRecord[MUTATION](bytes, schemaId, HEADER_SIZE)
    val isUpdate = isRecordAnUpdate(record)
    
    if (isUpdate && m.runtimeClass == classOf[UpdateMutation]) {
      deserializeUpdateMutation(record).asInstanceOf[MUTATION]
    } else {
      deserializeSingleValuedMutation(record).asInstanceOf[MUTATION]
    }
  }

  /**
   * Checks if the record contains a single valued row (insert, update) or multiple values
   * in the case of a delete. This is done by inspecting the record for at least one field
   * in the form of "old.$fieldName" and "new.$fieldName". If this property is satisfied we
   * consider the record to represent an update.
   *
   * @param record to check
   * @return true if the record is an update, false otherwise
   */
  private def isRecordAnUpdate(record: GenericData.Record): Boolean = {
    val fields = record.getSchema.getFields().asScala
    val oldField = fields.find(_.name().startsWith("old."))

    if (oldField.isDefined) {
      val field = oldField.get.name().substring(4)
      val newField = fields.find(_.name().equals(s"new.$field"))
      newField.isDefined
    } else {
      false
    }
  }

  /**
   * Reads the header and returns it (magic, schemdId).
   *
   * @param bytes to read the header from
   * @return the header (magic, schemaId) tuple optionally, None otherwise
   */
  private def readHeader(bytes: Array[Byte]): Option[(Byte, Short)] = {
    val magic = bytes(0)
    val schemaId = getSchemaIdFromBytes(bytes, 1)
    Some((magic, schemaId))
  }

  /**
   * Reads database, table, and tableId fields from the record and returns them.
   *
   * @param record Avro record to read metadata from
   * @return a tuple with (database, table, tableId) optionally, None otherwise
   */
  private def readMetadata(record: GenericData.Record): Option[(String, String, JLong)] = Some(
    record.get("database").asInstanceOf[String],
    record.get("table").asInstanceOf[String],
    record.get("tableId").asInstanceOf[JLong])

  /**
   * Attempts to read out an Avro record.
   *
   * @param bytes the byte array containing the serialized Avro record
   * @param schemaId the schema ID to use when deserializing the byte array
   * @param offset the offset into the array to start reading data from
   * @return an Avro generic record for the given bytes
   */
  private def readRecord[MUTATION >: Mutation[_]](bytes: Array[Byte], schemaId: Short, offset: Int)(implicit r: reflect.ClassTag[MUTATION]): GenericData.Record = {
    val topic = topicFor(schemaId, r.runtimeClass.asInstanceOf[Class[Mutation[_]]])
    val schema = schemaRepoClient.getLatestSchema(topic).get
    val decoder = DecoderFactory.get().binaryDecoder(Array[Byte](), null)
    val reader = new GenericDatumReader[GenericRecord](schema)
    val record = new GenericData.Record(schema)
    reader.setSchema(schema)
    reader.read(record, DecoderFactory.get().binaryDecoder(bytes, offset, bytes.length - offset, decoder))

    record
  }

  /**
   * Creates an insert or update mutation from an Avro generic record.
   *
   * NOTE: This does not set the primary key yet.
   *
   * @param record to turn into a Mutation
   * @return the InsertMutation or DeleteMutation as a SingleValuedMutation
   */
  private def deserializeSingleValuedMutation[MUTATION >: Mutation[_]](record: GenericData.Record)(implicit m: Manifest[MUTATION]): MUTATION = {
    // TODO: get this properly
    val primaryKey: Option[PrimaryKey] = None
    val (tableDb, tableName, tableId) = readMetadata(record).get
    val columns = readColumns(record)
    val columnsMetadata = columns.map(_.metadata)
    val table = Table(tableId, tableName, tableDb, columnsMetadata.toList, primaryKey)
    val row = Row(table, columns.map(c ⇒ c.metadata.name -> c).toMap)
    m.erasure.getConstructor(classOf[Table], classOf[List[Row]]).newInstance(table, List(row)).asInstanceOf[MUTATION]
  }

  /**
   * Creates an update mutation from an Avro record.
   *
   * NOTE: This does not set the primary key yet.
   *
   * @param record to turn into a Mutation
   * @return the UpdateMutation
   */
  private def deserializeUpdateMutation(record: GenericData.Record): UpdateMutation = {
    // TODO: get this properly
    val primaryKey: Option[PrimaryKey] = None
    val (tableDb, tableName, tableId) = readMetadata(record).get
    val columnsOld = readColumns(record, PrependingNameMapper("old."))
    val columnsNew = readColumns(record, PrependingNameMapper("new."))
    val columnsMetadata = columnsOld.map(_.metadata)
    val table = Table(tableId, tableName, tableDb, columnsMetadata.toList, primaryKey)
    val rowOld = Row(table, columnsOld.map(c ⇒ c.metadata.name -> c).toMap)
    val rowNew = Row(table, columnsNew.map(c ⇒ c.metadata.name -> c).toMap)
    UpdateMutation(table, List((rowOld, rowNew)))
  }

  /**
   * Gets a schema ID from an array of bytes.
   * @param bytes containing the schema ID
   * @param offset within the byte array to start reading from
   * @return the schema ID
   */
  private def getSchemaIdFromBytes(bytes: Array[Byte], offset: Int): Short = byteArray2Short(bytes, 1)

  /**
   * Turns a byte array into a short.
   *
   * @param data bytes to transform
   * @param offset offset within the byte array to read from
   * @return short value
   */
  private def byteArray2Short(data: Array[Byte], offset: Int) = (((data(offset) << 8)) | ((data(offset + 1) & 0xff))).toShort
}

/**
 * Deserializes an array of bytes into an InsertMutation, UpdateMutation, or DeleteMutation
 * assuming the data was encoded with the "generic" insert, update, and delete schemas.
 */
trait AvroGenericRecordMutationDeserializer extends AvroRecordMutationDeserializer {

  /**
   * Uses the generic in memory repo that generically maps insert, update, and
   * delete mutations to Avro schemas.
   */
  override protected val schemaRepoClient = new GenericInMemorySchemaRepo()

  /**
   * Looks for fields called "integers", "strings", and "longs" and pulls them out as
   * Avro maps containing key value pairs for the original table's columns.
   *
   * @param record to read columns from.
   * @param nameMapper to apply on the column names before
   * @return
   */
  override protected def readColumns(record: GenericData.Record, nameMapper: NameMapper = IdentityNameMapper): Seq[Column] = {
    val integers = record.get(nameMapper("integers")).asInstanceOf[java.util.Map[String, Integer]]
    val strings = record.get(nameMapper("strings")).asInstanceOf[java.util.Map[String, String]]
    val longs = record.get(nameMapper("longs")).asInstanceOf[java.util.Map[String, JLong]]

    createColumns(integers, ColumnType.INT24) ++
      createColumns(strings, ColumnType.VARCHAR) ++
      createColumns(longs, ColumnType.LONG)
  }

  /**
   * Creates Column instances from a map of columns names to Serializable's.
   *
   * @param columns used to create Column instances
   * @param colType the type these columns have
   * @return a sequence of Column's
   */
  private def createColumns(columns: JMap[String, _], colType: ColumnType.EnumVal): Seq[Column] = {
    columns.asScala.map(kv ⇒ {
      val colName = kv._1
      // TODO: we don't have a good way of finding this out yet, use MetadataManager?
      val isPrimaryKey = false
      val meta = ColumnMetadata(colName, colType, isPrimaryKey)
      // TODO: this is ugly
      Column(meta, kv._2.asInstanceOf[java.io.Serializable])
    }).toSeq
  }
}

trait AvroVersionedSpecificRecordMutationDeserializer extends AvroRecordMutationDeserializer {

  override protected def readColumns(record: GenericData.Record, nameMapper: NameMapper = IdentityNameMapper): Seq[Column] = {
    ???
  }

}

