package mypipe.avro

import org.apache.avro.Schema
import mypipe.api._
import mypipe.api.UpdateMutation
import scala.Some
import mypipe.api.DeleteMutation
import mypipe.api.InsertMutation
import org.apache.avro.generic.{ GenericRecord, GenericDatumWriter, GenericData }
import java.io.ByteArrayOutputStream
import org.apache.avro.io.EncoderFactory

trait AvroGenericRecordMutationSerializer extends AvroRecordMutationSerializer {

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

  override protected def getSchemaAndIdForMutation(mutation: Mutation[_]): Option[(Short, Schema)] = mutation match {
    case m: InsertMutation ⇒ Some(INSERT -> insertSchema)
    case m: UpdateMutation ⇒ Some(UPDATE -> updateSchema)
    case m: DeleteMutation ⇒ Some(DELETE -> deleteSchema)
  }

  override protected def writeColumns(record: GenericData.Record, columns: Map[String, Column], nameMapper: NameMapper = IdentityNameMapper) {
    val cols = columns.values.groupBy(_.metadata.colType)

    val integers = new java.util.HashMap[String, Int]()
    val strings = new java.util.HashMap[String, String]()
    val longs = new java.util.HashMap[String, java.lang.Long]()

    record.put(nameMapper("integers"), integers)
    record.put(nameMapper("strings"), strings)
    record.put(nameMapper("longs"), longs)

    cols.foreach(_ match {

      case (ColumnType.INT24, columns) ⇒
        columns.foreach(c ⇒ integers.put(c.metadata.name, c.value[Int]))

      case (ColumnType.VARCHAR, columns) ⇒
        columns.foreach(c ⇒ strings.put(c.metadata.name, c.value[String]))

      case (ColumnType.LONG, columns) ⇒
        columns.foreach(c ⇒ {
          // this damn thing can come in as an Integer or Long
          val v = c.value match {
            case i: java.lang.Integer ⇒ new java.lang.Long(i.toLong)
            case l: java.lang.Long    ⇒ l
          }

          longs.put(c.metadata.name, v)
        })

      case _ ⇒ // unsupported
    })
  }
}
