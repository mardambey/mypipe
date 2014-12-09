package mypipe.kafka

import mypipe.api.event.Mutation

import scala.reflect.runtime.universe._
import org.slf4j.LoggerFactory
import mypipe.avro.schema.GenericSchemaRepository
import org.apache.avro.Schema
import org.apache.avro.specific.SpecificRecord
import mypipe.avro.AvroVersionedRecordDeserializer

abstract class KafkaMutationAvroConsumer[InsertMutationType <: SpecificRecord, UpdateMutationType <: SpecificRecord, DeleteMutationType <: SpecificRecord, SchemaId](
  topic: String,
  zkConnect: String,
  groupId: String,
  schemaIdSizeInBytes: Int)(insertCallback: (InsertMutationType) ⇒ Boolean,
                            updateCallback: (UpdateMutationType) ⇒ Boolean,
                            deleteCallback: (DeleteMutationType) ⇒ Boolean,
                            implicit val insertTag: TypeTag[InsertMutationType],
                            implicit val updateTag: TypeTag[UpdateMutationType],
                            implicit val deletetag: TypeTag[DeleteMutationType])
    extends KafkaConsumer(topic, zkConnect, groupId) {

  // abstract fields and methods
  protected val schemaRepoClient: GenericSchemaRepository[SchemaId, Schema]
  protected def bytesToSchemaId(bytes: Array[Byte], offset: Int): SchemaId
  protected def avroSchemaSubjectForMutationByte(byte: Byte): String

  protected val logger = LoggerFactory.getLogger(getClass.getName)
  protected val headerLength = PROTO_HEADER_LEN_V0 + schemaIdSizeInBytes

  val insertDeserializer: AvroVersionedRecordDeserializer[InsertMutationType]
  val updateDeserializer: AvroVersionedRecordDeserializer[UpdateMutationType]
  val deleteDeserializer: AvroVersionedRecordDeserializer[DeleteMutationType]

  override def onEvent(bytes: Array[Byte]): Boolean = try {

    val magicByte = bytes(0)

    if (magicByte != PROTO_MAGIC_V0) {
      logger.error(s"We have encountered an unknown magic byte! Magic Byte: $magicByte")
      false
    } else {
      val mutationType = bytes(1)
      val schemaId = bytesToSchemaId(bytes, PROTO_HEADER_LEN_V0)

      val continue = mutationType match {
        case Mutation.InsertByte ⇒ schemaRepoClient
          .getSchema(avroSchemaSubjectForMutationByte(Mutation.InsertByte), schemaId)
          .map(insertDeserializer.deserialize(_, bytes, headerLength).map(m ⇒ insertCallback(m)))
          .getOrElse(None)

        case Mutation.UpdateByte ⇒ schemaRepoClient
          .getSchema(avroSchemaSubjectForMutationByte(Mutation.UpdateByte), schemaId)
          .map(updateDeserializer.deserialize(_, bytes, headerLength).map(m ⇒ updateCallback(m)))
          .getOrElse(None)

        case Mutation.DeleteByte ⇒ schemaRepoClient
          .getSchema(avroSchemaSubjectForMutationByte(Mutation.DeleteByte), schemaId)
          .map(deleteDeserializer.deserialize(_, bytes, headerLength).map(m ⇒ deleteCallback(m)))
          .getOrElse(None)
      }

      continue.getOrElse(false)
    }
  } catch {
    case e: Exception ⇒
      log.error("Could not run callback on " + bytes.mkString + " => " + e.getMessage + "\n" + e.getStackTraceString)
      false
  }
}

