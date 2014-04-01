package mypipe.avro

import org.apache.avro.specific.{ SpecificDatumWriter, SpecificRecord }
import mypipe.avro.schema.SchemaRepository
import org.apache.avro.Schema
import mypipe.api.Serializer
import java.util.logging.Logger
import org.apache.avro.io.EncoderFactory
import java.io.ByteArrayOutputStream

class AvroVersionedRecordSerializer[InputRecord <: SpecificRecord](schemaRepoClient: SchemaRepository[Short, Schema])
    extends Serializer[InputRecord, Array[Byte]] {

  protected val logger = Logger.getLogger(getClass.getName)
  protected val magicByteForVersionZero: Byte = 0x0.toByte
  protected val headerLengthForVersionZero: Int = 3
  protected val encoderFactory = EncoderFactory.get()
  protected val encoder = encoderFactory.binaryEncoder(new ByteArrayOutputStream(), null)
  protected val writer = new SpecificDatumWriter[InputRecord]()

  private def short2ByteArray(s: Short) = Array[Byte](((s & 0xFF00) >> 8).toByte, (s & 0x00FF).toByte)

  override def serialize(topic: String, inputRecord: InputRecord): Option[Array[Byte]] = {
    for (
      schema ← schemaRepoClient.getLatestSchema(topic);
      schemaId ← schemaRepoClient.getSchemaId(topic, schema)
    ) yield {
      writer.setSchema(schema)
      val out = new ByteArrayOutputStream()
      out.write(new Array[Byte](magicByteForVersionZero))
      out.write(short2ByteArray(schemaId))
      writer.write(inputRecord, encoderFactory.binaryEncoder(out, encoder))
      out.toByteArray
    }
  }
}