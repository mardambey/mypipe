package mypipe.avro

import mypipe.api._
import mypipe.api.UpdateMutation
import mypipe.api.InsertMutation
import org.apache.avro.Schema
import org.apache.avro.generic.{ GenericRecord, GenericDatumWriter, GenericData }
import java.io.ByteArrayOutputStream
import org.apache.avro.io.EncoderFactory

trait NameMappers {
  type NameMapper = (String) ⇒ String

  def IdentityNameMapper(s: String) = s
  def PrependingNameMapper(prepend: String)(s: String) = prepend + s
  def AppendingNameMapper(append: String)(s: String) = s + append
}

/**
 * Serializes Mutation instances into byte arrays via Avro.
 *
 * The produces byte arrays contain:
 * -----------------
 * |MAGIC | 1 byte |
 * |SID   | 2 bytes|
 * |DATA  | N bytes|
 * -----------------
 * The DATA portion is Avro encoded.
 */
trait AvroRecordMutationSerializer extends MutationSerializer[Array[Byte]] with NameMappers {

  val MAGIC: Byte = 0

  /**
   * Fetches the Avro schema and schema ID for the given mutation.
   *
   * @param mutation to fetch shcmea and ID for
   * @return optionally returns a tuple with the ID and schema, None otherwise
   */
  protected def getSchemaAndIdForMutation(mutation: Mutation[_]): Option[(Short, Schema)]

  /**
   * Serializes an InsertMutatin to a byte array.
   *
   * @param schema the Avro schema to use
   * @param i the InsertMutation to serialize
   * @return the mutation as bytes
   */
  protected def serializeInsertMutation(schema: Schema, i: InsertMutation): Array[Byte] = serializeSingleValuedMutation(schema, i)

  /**
   * Serializes an DeleteMutation to a byte array.
   *
   * @param schema the Avro schema to use
   * @param d the DeleteMutation to serialize
   * @return the mutation as bytes
   */
  protected def serializeDeleteMutation(schema: Schema, d: DeleteMutation): Array[Byte] = serializeSingleValuedMutation(schema, d)

  /**
   * Writes the given columns into the given record optionally applying a name mapper on the column names before
   * writing them into the record.
   *
   * @param record to write into
   * @param columns to serialize and write into the record
   * @param nameMapper optionally used to change the column names in the record
   */
  protected def writeColumns(record: GenericData.Record, columns: Map[String, Column], nameMapper: NameMapper = IdentityNameMapper)

  def serialize(mutation: Mutation[_]): Array[Byte] = {

    // TODO: make this cleaner
    val sid = getSchemaAndIdForMutation(mutation)

    if (sid.isDefined) {
      addHeader(sid.get._1, mutation match {
        case i: InsertMutation ⇒ serializeInsertMutation(sid.get._2, i)
        case u: UpdateMutation ⇒ serializeUpdateMutation(sid.get._2, u)
        case d: DeleteMutation ⇒ serializeDeleteMutation(sid.get._2, d)
      })
    } else {
      // TODO: complain
      Array[Byte]()
    }
  }

  // TODO: copying sucks, re-implement this
  private def addHeader(schemaId: Short, bytes: Array[Byte]): Array[Byte] = {
    Array(MAGIC) ++ short2ByteArray(schemaId) ++ bytes
  }

  private def short2ByteArray(s: Short) = Array[Byte](((s & 0xFF00) >> 8).toByte, (s & 0x00FF).toByte)

  private def writeMetadata(record: GenericData.Record, mutation: Mutation[_]) {
    record.put("database", mutation.table.db)
    record.put("table", mutation.table.name)
    record.put("tableId", mutation.table.id)
  }

  protected def serializeUpdateMutation(schema: Schema, u: UpdateMutation): Array[Byte] = {
    toByteArray(schema, u) { record =>
      writeMetadata(record, u)
      writeColumns(record, u.rows.head._1.columns, PrependingNameMapper("old."))
      writeColumns(record, u.rows.head._2.columns, PrependingNameMapper("new."))
    }
  }

  private def toByteArray(schema: Schema, mutation: Mutation[_])(code: (GenericData.Record) ⇒ Unit) = {
    val record = new GenericData.Record(schema)
    val writer = new GenericDatumWriter[GenericRecord](schema)    val out = new ByteArrayOutputStream()
    val encoder = EncoderFactory.get().binaryEncoder(out, null)

    code(record)

    writer.write(record, encoder)
    encoder.flush()
    out.close()
    out.toByteArray
  }

  private def serializeSingleValuedMutation(schema: Schema, mutation: SingleValuedMutation): Array[Byte] = {

    toByteArray(schema, mutation) { record =>
      record.put("database", mutation.table.db)
      record.put("table", mutation.table.name)
      record.put("tableId", mutation.table.id)
      writeColumns(record, mutation.rows.head.columns)
    }
  }
}
