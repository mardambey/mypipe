package mypipe.kafka.consumer

import kafka.serializer.Decoder
import mypipe.api.event.Mutation
import mypipe.avro._
import mypipe.avro.schema.{ AvroSchemaUtils, GenericSchemaRepository }
import mypipe.kafka._
import org.apache.avro.Schema
import org.apache.avro.specific.SpecificRecord
import org.slf4j.LoggerFactory

case class KafkaGenericAvroDecoder() extends Decoder[SpecificRecord] {

  protected val logger = LoggerFactory.getLogger(getClass.getName)

  protected val schemaRepoClient: GenericSchemaRepository[Short, Schema] = GenericInMemorySchemaRepo
  protected def bytesToSchemaId(bytes: Array[Byte], offset: Int): Short = byteArray2Short(bytes, offset)
  protected def byteArray2Short(data: Array[Byte], offset: Int) = (data(offset) << 8 | data(offset + 1) & 0xff).toShort
  protected def avroSchemaSubjectForMutationByte(byte: Byte): String = AvroSchemaUtils.genericSubject(Mutation.byteToString(byte))
  protected val schemaIdSizeInBytes = 2
  protected val headerLength = PROTO_HEADER_LEN_V0 + schemaIdSizeInBytes

  lazy val insertDeserializer = new AvroVersionedRecordDeserializer[InsertMutation]()
  lazy val updateDeserializer = new AvroVersionedRecordDeserializer[UpdateMutation]()
  lazy val deleteDeserializer = new AvroVersionedRecordDeserializer[DeleteMutation]()

  override def fromBytes(bytes: Array[Byte]): SpecificRecord = {
    val magicByte = bytes(0)

    if (magicByte != PROTO_MAGIC_V0) {
      logger.error(s"We have encountered an unknown magic byte! Magic Byte: $magicByte")
      null
    } else {
      val mutationType = bytes(1)
      val schemaId = bytesToSchemaId(bytes, PROTO_HEADER_LEN_V0)

      val record = mutationType match {
        case Mutation.InsertByte ⇒ schemaRepoClient
          .getSchema(avroSchemaSubjectForMutationByte(Mutation.InsertByte), schemaId)
          .flatMap(insertDeserializer.deserialize(_, bytes, headerLength))
          .orNull

        case Mutation.UpdateByte ⇒ schemaRepoClient
          .getSchema(avroSchemaSubjectForMutationByte(Mutation.UpdateByte), schemaId)
          .flatMap(updateDeserializer.deserialize(_, bytes, headerLength))
          .orNull

        case Mutation.DeleteByte ⇒ schemaRepoClient
          .getSchema(avroSchemaSubjectForMutationByte(Mutation.DeleteByte), schemaId)
          .flatMap(deleteDeserializer.deserialize(_, bytes, headerLength))
          .orNull
      }

      logger.debug("Decoded the following bytes: {}", record)

      record
    }
  }
}
