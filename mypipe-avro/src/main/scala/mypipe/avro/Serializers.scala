package mypipe.avro

import org.apache.avro.Schema
import org.apache.avro.generic.{ GenericData, GenericRecord, GenericDatumWriter }
import mypipe.api._
import java.io.ByteArrayOutputStream
import org.apache.avro.io.EncoderFactory
import scala.collection.JavaConverters._

trait AvroVersionedSpecificRecordMutationSerializer extends MutationSerializer[Array[Byte]] {

  def serialize(mutation: Mutation[_]): Array[Byte] = {
    ???
  }
}

trait AvroGenericRecordMutationSerializer extends MutationSerializer[Array[Byte]] {

  val insertSchemaFile: String = "/InsertMutation.avsc"
  val updateSchemaFile: String = "/UpdateMutation.avsc"
  val deleteSchemaFile: String = "/DeleteMutation.avsc"

  val insertSchema = try { Schema.parse(getClass.getResourceAsStream(insertSchemaFile)) } catch { case e: Exception ⇒ println("Failed on insert: " + e.getMessage); null }
  val updateSchema = try { Schema.parse(getClass.getResourceAsStream(updateSchemaFile)) } catch { case e: Exception ⇒ println("Failed on update: " + e.getMessage); null }
  val deleteSchema = try { Schema.parse(getClass.getResourceAsStream(deleteSchemaFile)) } catch { case e: Exception ⇒ println("Failed on delete: " + e.getMessage); null }

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
    record.put("integers", new java.util.HashMap[String, Int]())
    record.put("strings", new java.util.HashMap[String, String]())

    // we'll probably want to collapse certain data types together here
    // for example, varchar and text might map to the string map
    val cols = i.rows.head.columns.values.groupBy(_.metadata.colType)

    cols.foreach(_ match {

      case (ColumnType.INT24, columns) ⇒
        val map = columns.map(c ⇒ c.metadata.name -> c.value[Int]).toMap.asJava
        record.put("integers", map)

      case (ColumnType.VARCHAR, columns) ⇒
        val map = columns.map(c ⇒ c.metadata.name -> c.value[String]).toMap.asJava
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
