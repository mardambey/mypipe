package mypipe.kafka.consumer

import mypipe.api.event.Mutation
import mypipe.avro.schema.{AvroSchemaUtils, GenericSchemaRepository}
import org.apache.avro.Schema
import org.apache.avro.specific.SpecificRecord

import scala.reflect.runtime.universe._

case class KafkaSpecificAvroDecoder[InsertMutationType <: SpecificRecord, UpdateMutationType <: SpecificRecord, DeleteMutationType <: SpecificRecord](database: String, table: String, schemaRepoClient: GenericSchemaRepository[Short, Schema])(implicit override val insertTag: TypeTag[InsertMutationType], implicit override val updateTag: TypeTag[UpdateMutationType], implicit override val deleteTag: TypeTag[DeleteMutationType])
    extends KafkaAvroDecoder[InsertMutationType, UpdateMutationType, DeleteMutationType]() {

  protected def avroSchemaSubjectForMutationByte(byte: Byte): String = AvroSchemaUtils.specificSubject(database, table, Mutation.byteToString(byte))
}
