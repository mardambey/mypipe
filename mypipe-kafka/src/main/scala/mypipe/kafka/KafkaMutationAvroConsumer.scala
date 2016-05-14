package mypipe.kafka

import kafka.serializer._
import scala.reflect.runtime.universe._

import org.slf4j.LoggerFactory
import org.apache.avro.Schema
import org.apache.avro.specific.SpecificRecord

import mypipe.avro.AvroVersionedRecordDeserializer
import mypipe.avro.schema.GenericSchemaRepository

abstract class KafkaMutationAvroConsumer[InsertMutationType <: SpecificRecord, UpdateMutationType <: SpecificRecord, DeleteMutationType <: SpecificRecord, SchemaId](
  topic: String,
  zkConnect: String,
  groupId: String,
  schemaIdSizeInBytes: Int,
  valueDecoder: Decoder[SpecificRecord])(insertCallback: (InsertMutationType) ⇒ Boolean,
                            updateCallback: (UpdateMutationType) ⇒ Boolean,
                            deleteCallback: (DeleteMutationType) ⇒ Boolean,
                            implicit val insertTag: TypeTag[InsertMutationType],
                            implicit val updateTag: TypeTag[UpdateMutationType],
                            implicit val deletetag: TypeTag[DeleteMutationType])
    extends KafkaConsumer[SpecificRecord](topic, zkConnect, groupId, valueDecoder) {

  protected val schemaRepoClient: GenericSchemaRepository[SchemaId, Schema]
  protected val logger = LoggerFactory.getLogger(getClass.getName)
  protected val headerLength = PROTO_HEADER_LEN_V0 + schemaIdSizeInBytes

  protected def bytesToSchemaId(bytes: Array[Byte], offset: Int): SchemaId
  protected def avroSchemaSubjectForMutationByte(byte: Byte): String

  val insertDeserializer: AvroVersionedRecordDeserializer[InsertMutationType]
  val updateDeserializer: AvroVersionedRecordDeserializer[UpdateMutationType]
  val deleteDeserializer: AvroVersionedRecordDeserializer[DeleteMutationType]

  override def onEvent(mutation: SpecificRecord): Boolean = try {
    mutation match {
      case m: InsertMutationType => insertCallback(m)
      case m: UpdateMutationType => updateCallback(m)
      case m: DeleteMutationType => deleteCallback(m)
      case _ => false
    }
  } catch {
    case e: Exception ⇒
      log.error("Could not run callback on {} => {}: {}", mutation, e.getMessage, e.getStackTrace.mkString(sys.props("line.separator")))
      false
  }
}

