package mypipe.sqs

import scala.reflect.runtime.universe._
import mypipe.avro.{ AvroVersionedRecordDeserializer, InsertMutation, UpdateMutation, DeleteMutation }

import mypipe.sqs.SQSGenericMutationAvroConsumer._

object SQSGenericMutationAvroConsumer {
  type GenericInsertMutationCallback = (InsertMutation) ⇒ Boolean
  type GenericUpdateMutationCallback = (UpdateMutation) ⇒ Boolean
  type GenericDeleteMutationCallback = (DeleteMutation) ⇒ Boolean
}

abstract class SQSGenericMutationAvroConsumer[SchemaId](
  topic: String,
  sqsQueue: String,
  groupId: String,
  schemaIdSizeInBytes: Int)(insertCallback: GenericInsertMutationCallback,
                            updateCallback: GenericUpdateMutationCallback,
                            deleteCallback: GenericDeleteMutationCallback)

    extends SQSMutationAvroConsumer[mypipe.avro.InsertMutation, mypipe.avro.UpdateMutation, mypipe.avro.DeleteMutation, SchemaId](
      topic, sqsQueue, groupId, schemaIdSizeInBytes)(
      insertCallback, updateCallback, deleteCallback,
      implicitly[TypeTag[InsertMutation]],
      implicitly[TypeTag[UpdateMutation]],
      implicitly[TypeTag[DeleteMutation]]) {

  override lazy val insertDeserializer = new AvroVersionedRecordDeserializer[InsertMutation]()
  override lazy val updateDeserializer = new AvroVersionedRecordDeserializer[UpdateMutation]()
  override lazy val deleteDeserializer = new AvroVersionedRecordDeserializer[DeleteMutation]()
}

