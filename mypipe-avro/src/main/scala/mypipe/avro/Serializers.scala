package mypipe.avro

import org.apache.avro.Schema
import org.apache.avro.generic.{ GenericData, GenericRecord, GenericDatumWriter }
import mypipe.api._
import java.io.ByteArrayOutputStream
import org.apache.avro.io.EncoderFactory
import mypipe.avro.schema.SchemaRepo

trait AvroVersionedSpecificRecordMutationSerializer extends AvroRecordMutationSerializer {

  val repo = SchemaRepo

  override protected def serializeInsertMutation(schema: Schema, i: InsertMutation): Array[Byte] = {
    val record = new GenericData.Record(schema)
    val writer = new GenericDatumWriter[GenericRecord](schema)

    // TODO: write data out

    val out = new ByteArrayOutputStream()
    val encoder = EncoderFactory.get().binaryEncoder(out, null)
    writer.write(record, encoder)
    encoder.flush()
    out.close()
    out.toByteArray
  }

  override protected def serializeUpdateMutation(schema: Schema, u: UpdateMutation): Array[Byte] = ???
  override protected def serializeDeleteMutation(schema: Schema, d: DeleteMutation): Array[Byte] = ???

  override protected def getSchemaAndIdForMutation(mutation: Mutation[_]): Option[(Short, Schema)] = {

    // FIXME: this should be pulled out into a common call
    val topicName = mutation.table.db + "__" + mutation.table.name

    repo.getLatestSchema(topicName) match {
      case Some(schema) ⇒ {
        val id = repo.getSchemaId(topicName, schema)

        if (id.isDefined) Some(id.get -> schema)
        else None
      }

      case None ⇒ None

    }
  }
}

trait AvroRecordMutationSerializer extends MutationSerializer[Array[Byte]] {
  val MAGIC: Byte = 0

  // TODO: copying sucks, reimplement this
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

  protected def serializeInsertMutation(schema: Schema, i: InsertMutation): Array[Byte]
  protected def serializeUpdateMutation(schema: Schema, u: UpdateMutation): Array[Byte]
  protected def serializeDeleteMutation(schema: Schema, d: DeleteMutation): Array[Byte]

  protected def getSchemaAndIdForMutation(mutation: Mutation[_]): Option[(Short, Schema)]
}

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

  // writers used to serialize the mutaions
  val insertWriter = new GenericDatumWriter[GenericRecord](insertSchema)
  val updateWriter = new GenericDatumWriter[GenericRecord](updateSchema)
  val deleteWriter = new GenericDatumWriter[GenericRecord](deleteSchema)

  protected def getSchemaAndIdForMutation(mutation: Mutation[_]): Option[(Short, Schema)] = mutation match {
    case m: InsertMutation ⇒ Some(INSERT -> insertSchema)
    case m: UpdateMutation ⇒ Some(UPDATE -> updateSchema)
    case m: DeleteMutation ⇒ Some(DELETE -> deleteSchema)
  }

  protected def serializeInsertMutation(schema: Schema, i: InsertMutation): Array[Byte] = {
    val record = new GenericData.Record(schema)
    val integers = new java.util.HashMap[String, Int]()
    val strings = new java.util.HashMap[String, String]()
    val longs = new java.util.HashMap[String, java.lang.Long]()

    record.put("database", i.table.db)
    record.put("table", i.table.name)
    record.put("integers", integers)
    record.put("strings", strings)
    record.put("longs", longs)

    // we'll probably want to collapse certain data types together here
    // for example, varchar and text might map to the string map
    val cols = i.rows.head.columns.values.groupBy(_.metadata.colType)

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

    val out = new ByteArrayOutputStream()
    val encoder = EncoderFactory.get().binaryEncoder(out, null)
    insertWriter.write(record, encoder)
    encoder.flush()
    out.close()
    out.toByteArray
  }

  protected def serializeUpdateMutation(schema: Schema, u: UpdateMutation): Array[Byte] = ???
  protected def serializeDeleteMutation(schema: Schema, d: DeleteMutation): Array[Byte] = ???
}
