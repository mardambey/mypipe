package mypipe.avro

import mypipe.api.event.Serializer
import org.apache.avro.specific.{SpecificDatumWriter, SpecificRecord}
import mypipe.avro.schema.SchemaRepository
import org.apache.avro.Schema
import java.util.logging.Logger
import org.apache.avro.io.EncoderFactory
import java.io.ByteArrayOutputStream

class AvroVersionedRecordSerializer[InputRecord <: SpecificRecord](schemaRepoClient: SchemaRepository[Short, Schema])
    extends Serializer[InputRecord, Array[Byte]] {

  protected val logger = Logger.getLogger(getClass.getName)
  protected val magicByteForVersionZero: Byte = 0x0.toByte
  protected val headerLengthForVersionZero: Int = 3
  protected val encoderFactory = EncoderFactory.get()

  private def short2ByteArray(s: Short) = Array[Byte](((s & 0xFF00) >> 8).toByte, (s & 0x00FF).toByte)

  override def serialize(topic: String, inputRecord: InputRecord): Option[Array[Byte]] = {
    for (
      schema ← schemaRepoClient.getLatestSchema(topic);
      schemaId ← schemaRepoClient.getSchemaId(topic, schema)
    ) yield {
      val writer = new SpecificDatumWriter[InputRecord](schema)
      val out = new ByteArrayOutputStream()
      out.write(magicByteForVersionZero)
      out.write(short2ByteArray(schemaId))
      val enc = encoderFactory.binaryEncoder(out, null)
      writer.write(inputRecord, enc)
      enc.flush
      out.toByteArray
    }
  }
}