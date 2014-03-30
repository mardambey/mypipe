package mypipe.avro

import mypipe.api._
import org.apache.avro.io.DecoderFactory
import org.apache.avro.generic.{GenericData, GenericRecord, GenericDatumReader}
import org.apache.avro.Schema
import mypipe.api.Column
import scala.collection.JavaConverters._
import java.util.{Map => JMap}
import java.lang.{Long => JLong}

trait AvroRecordMutationDeserializer extends MutationDeserializer[Array[Byte]] with NameMappers {

  val MAGIC: Byte = 0
  val HEADER_SIZE = 3

  protected def deserializeInsertMutation(recored: GenericData.Record): InsertMutation
  protected def deserializeUpdateMutation(recored: GenericData.Record): UpdateMutation
  protected def deserializeDeleteMutation(recored: GenericData.Record): DeleteMutation
  protected def readColumns(record: GenericData.Record, nameMapper: NameMapper = IdentityNameMapper) : Seq[Column]
  protected def getSchemaById(schemaId: Short): Schema

  private def readHeader(bytes: Array[Byte]): Option[(Byte, Short)] = {
    val magic = bytes(0)
    val schemaId = getSchemaIdFromBytes(bytes, 1)
    Some((magic, schemaId))
  }

  private def readRecord(bytes: Array[Byte], schemaId: Short, offset: Int): GenericData.Record = {
    val schema = getSchemaById(schemaId)
    val decoder = DecoderFactory.get().binaryDecoder(Array[Byte](), null)
    val reader = new GenericDatumReader[GenericRecord](schema)
    val record = new GenericData.Record(schema)
    reader.setSchema(schema)
    reader.read(record, DecoderFactory.get().binaryDecoder(bytes, offset, bytes.length - offset, decoder))

    record
  }

  def deserialize(bytes: Array[Byte]): Mutation[_] = {
    val (magic, schemaId) = readHeader(bytes).get
    val record = readRecord(bytes, schemaId, HEADER_SIZE)

    record.getSchema match {
      case insertSchema => deserializeInsertMutation(record)
      case updateSchema => deserializeUpdateMutation(record)
      case deleteSchema => deserializeDeleteMutation(record)
    }
  }

  private def getSchemaIdFromBytes(bytes: Array[Byte], offset: Int): Short = byteArray2Short(bytes, 1)
  private def byteArray2Short(data: Array[Byte], offset: Int) = (((data(offset) << 8)) | ((data(offset + 1) & 0xff))).toShort
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

  override def getSchemaById(schemaId: Short): Schema = schemaId match {
    case INSERT ⇒ insertSchema
    case UPDATE ⇒ updateSchema
    case DELETE ⇒ deleteSchema
  }

  protected def createColumnAndMetadata(columns: JMap[String, _], colType: ColumnType.EnumVal): Seq[(Column, ColumnMetadata)] = {
    columns.asScala.map(kv => {
      val colName = kv._1
      // TODO: we don't have a good way of finding this out yet, use MetadataManager?
      val isPrimaryKey = false
      val meta = ColumnMetadata(colName, colType, isPrimaryKey)
      // TODO: this is ugly
      val column = Column(meta, kv._2.asInstanceOf[java.io.Serializable])

      (column, meta)
    }).toSeq
  }

  private def readMetadata(record: GenericData.Record): Option[(String, String, JLong)] = Some(
    record.get("database").asInstanceOf[String],
    record.get("table").asInstanceOf[String],
    record.get("tableId").asInstanceOf[JLong])


  protected def readColumns(record: GenericData.Record, nameMapper: NameMapper = IdentityNameMapper) : Seq[Column] = {
    ???
  }

  def genericRecordToMutation(record: GenericData.Record): Mutation[_] = {
    record.getSchema match {
      case insertSchema ⇒ {

        val (tableDb, tableName, tableId) = readMetadata(record)


        val integers = record.get("integers").asInstanceOf[java.util.Map[String, Integer]]
        val strings = record.get("strings").asInstanceOf[java.util.Map[String, String]]
        val longs = record.get("longs").asInstanceOf[java.util.Map[String, JLong]]
        val columnsAndMetadata =
          createColumnAndMetadata(integers, ColumnType.INT24)   ++
          createColumnAndMetadata(strings,  ColumnType.VARCHAR) ++
          createColumnAndMetadata(longs,    ColumnType.LONG)

        val c = columnsAndMetadata.unzip
        val columns = c._1
        val columnMetadata = c._2


        // TODO: get this properly
        val primaryKey: Option[PrimaryKey] = None
        val table = Table(tableId, tableName, tableDb, columnMetadata.toList, primaryKey)
        val row = Row(table, columns.map(c => c.metadata.name -> c).toMap)

        InsertMutation(table, List(row))
      }

      case updateSchema ⇒
      case deleteSchema ⇒
    }

    ???
  }
}

trait AvroVersionedSpecificRecordMutationDeserializer extends AvroRecordMutationDeserializer

