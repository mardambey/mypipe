package mypipe.avro

import mypipe.api._
import mypipe.api.UpdateMutation
import mypipe.api.InsertMutation
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericRecord, GenericDatumWriter, GenericData}
import java.io.ByteArrayOutputStream
import org.apache.avro.io.EncoderFactory

trait AvroRecordMutationSerializer extends MutationSerializer[Array[Byte]] {
  val MAGIC: Byte = 0

  // TODO: copying sucks, re-implement this
  protected def addHeader(schemaId: Short, bytes: Array[Byte]): Array[Byte] = {
    Array(MAGIC, schemaId.toByte) ++ bytes
  }

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

  type NameMapper = (String*) => String

  def IdentityNameMapper(s: String) = s
  def PrependingNameMapper(prepend: String)(s: String) = prepend + s
  def AppendingNameMapper(append: String)(s: String) = s + append

  protected def serializeUpdateMutation(schema: Schema, u: UpdateMutation): Array[Byte] = {
    val record = new GenericData.Record(schema)
    val writer = new GenericDatumWriter[GenericRecord](schema)

    record.put("database", u.table.db)
    record.put("table", u.table.name)

    writeColumns(record, u.rows.head._1.columns, PrependingNameMapper("old."))
    writeColumns(record, u.rows.head._2.columns, PrependingNameMapper("new."))

    val out = new ByteArrayOutputStream()
    val encoder = EncoderFactory.get().binaryEncoder(out, null)
    writer.write(record, encoder)
    encoder.flush()
    out.close()
    out.toByteArray
  }

  protected def toByteArray(schema: Schema, record: GenericData.Record, mutation: SingleValuedMutation)(code: => Unit) = {
    val writer = new GenericDatumWriter[GenericRecord](schema)
    code
    writeColumns(record, mutation.rows.head.columns)

    val out = new ByteArrayOutputStream()
    val encoder = EncoderFactory.get().binaryEncoder(out, null)
    writer.write(record, encoder)
    encoder.flush()
    out.close()
    out.toByteArray
  }

  protected def serializeSingleValuedMutation(schema: Schema, mutation: SingleValuedMutation): Array[Byte] = {
    val record = new GenericData.Record(schema)

    toByteArray(schema, record, mutation) {
      record.put("database", mutation.table.db)
      record.put("table", mutation.table.name)
    }
  }

  protected def getSchemaAndIdForMutation(mutation: Mutation[_]): Option[(Short, Schema)]
  protected def serializeInsertMutation(schema: Schema, i: InsertMutation): Array[Byte] = serializeSingleValuedMutation(schema, i)
  protected def serializeDeleteMutation(schema: Schema, d: DeleteMutation): Array[Byte] = serializeSingleValuedMutation(schema, d)
  protected def writeColumns(record: GenericData.Record, columns: Map[String, Column], nameMapper: NameMapper = IdentityNameMapper)
}
