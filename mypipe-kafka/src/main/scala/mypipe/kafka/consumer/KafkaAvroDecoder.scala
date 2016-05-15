package mypipe.kafka.consumer

import kafka.serializer._

import mypipe.api.event.Mutation
import mypipe.avro.AvroVersionedRecordDeserializer
import mypipe.avro.schema.GenericSchemaRepository
import mypipe.kafka._
import org.apache.avro.Schema
import org.apache.avro.specific.SpecificRecord
import org.slf4j.LoggerFactory

import scala.reflect.runtime.universe._

abstract class KafkaAvroDecoder[InsertMutationType <: SpecificRecord, UpdateMutationType <: SpecificRecord, DeleteMutationType <: SpecificRecord]()
                                                                                                                                                 (implicit val insertTag: TypeTag[InsertMutationType], implicit val updateTag: TypeTag[UpdateMutationType], implicit val deleteTag: TypeTag[DeleteMutationType])
  extends Decoder[SpecificRecord] {

  protected val logger = LoggerFactory.getLogger(getClass.getName)

  protected def bytesToSchemaId(bytes: Array[Byte], offset: Int): Short = byteArray2Short(bytes, offset)
  protected def byteArray2Short(data: Array[Byte], offset: Int) = ((data(offset) << 8) | (data(offset + 1) & 0xff)).toShort
  protected val schemaIdSizeInBytes = 2
  protected val headerLength = PROTO_HEADER_LEN_V0 + schemaIdSizeInBytes

  protected lazy val insertDeserializer: AvroVersionedRecordDeserializer[InsertMutationType] = new AvroVersionedRecordDeserializer[InsertMutationType]()
  protected lazy val updateDeserializer: AvroVersionedRecordDeserializer[UpdateMutationType] = new AvroVersionedRecordDeserializer[UpdateMutationType]()
  protected lazy val deleteDeserializer: AvroVersionedRecordDeserializer[DeleteMutationType] = new AvroVersionedRecordDeserializer[DeleteMutationType]()

  // abstract members and methods
  protected val schemaRepoClient: GenericSchemaRepository[Short, Schema]
  protected def avroSchemaSubjectForMutationByte(byte: Byte): String

  override def fromBytes(bytes: Array[Byte]): SpecificRecord = {
    val magicByte = bytes(0)

    if (magicByte != PROTO_MAGIC_V0) {
      logger.error(s"We have encountered an unknown magic byte! Magic Byte: $magicByte")
      null
    } else {
      val mutationType = bytes(1)
      val schemaId = bytesToSchemaId(bytes, PROTO_HEADER_LEN_V0)

      mutationType match {
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
    }
  }
}
