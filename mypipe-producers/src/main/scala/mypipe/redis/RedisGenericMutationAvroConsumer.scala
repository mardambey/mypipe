package mypipe.redis

import scala.reflect.runtime.universe._
import mypipe.avro.{ AvroVersionedRecordDeserializer, InsertMutation, UpdateMutation, DeleteMutation }

import mypipe.redis.RedisGenericMutationAvroConsumer._

object RedisGenericMutationAvroConsumer {
  type GenericInsertMutationCallback = (InsertMutation) ⇒ Boolean
  type GenericUpdateMutationCallback = (UpdateMutation) ⇒ Boolean
  type GenericDeleteMutationCallback = (DeleteMutation) ⇒ Boolean
}

abstract class RedisGenericMutationAvroConsumer[SchemaId](
  topic: String,
  redisConnect: String,
  groupId: String,
  schemaIdSizeInBytes: Int)(insertCallback: GenericInsertMutationCallback,
                            updateCallback: GenericUpdateMutationCallback,
                            deleteCallback: GenericDeleteMutationCallback)

    extends RedisMutationAvroConsumer[mypipe.avro.InsertMutation, mypipe.avro.UpdateMutation, mypipe.avro.DeleteMutation, SchemaId](
      topic, redisConnect, groupId, schemaIdSizeInBytes)(
      insertCallback, updateCallback, deleteCallback,
      implicitly[TypeTag[InsertMutation]],
      implicitly[TypeTag[UpdateMutation]],
      implicitly[TypeTag[DeleteMutation]]) {

  override lazy val insertDeserializer = new AvroVersionedRecordDeserializer[InsertMutation]()
  override lazy val updateDeserializer = new AvroVersionedRecordDeserializer[UpdateMutation]()
  override lazy val deleteDeserializer = new AvroVersionedRecordDeserializer[DeleteMutation]()
}

