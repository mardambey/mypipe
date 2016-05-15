package mypipe.kafka.consumer

import mypipe.api.event.Mutation
import mypipe.avro._
import mypipe.avro.schema.{ AvroSchemaUtils, GenericSchemaRepository }
import org.apache.avro.Schema

case class KafkaGenericAvroDecoder() extends KafkaAvroDecoder[InsertMutation, UpdateMutation, DeleteMutation] {
  protected override val schemaRepoClient: GenericSchemaRepository[Short, Schema] = GenericInMemorySchemaRepo
  protected override def avroSchemaSubjectForMutationByte(byte: Byte): String = AvroSchemaUtils.genericSubject(Mutation.byteToString(byte))
}
