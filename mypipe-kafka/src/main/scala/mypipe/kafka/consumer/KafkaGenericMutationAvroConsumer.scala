package mypipe.kafka.consumer

import mypipe.avro.{DeleteMutation, InsertMutation, UpdateMutation}
import KafkaGenericMutationAvroConsumer.{GenericDeleteMutationCallback, GenericInsertMutationCallback, GenericUpdateMutationCallback}

import scala.reflect.runtime.universe._

object KafkaGenericMutationAvroConsumer {
  type GenericInsertMutationCallback = (InsertMutation) ⇒ Boolean
  type GenericUpdateMutationCallback = (UpdateMutation) ⇒ Boolean
  type GenericDeleteMutationCallback = (DeleteMutation) ⇒ Boolean
}

class KafkaGenericMutationAvroConsumer(topic: String, zkConnect: String, groupId: String)(insertCallback: GenericInsertMutationCallback, updateCallback: GenericUpdateMutationCallback, deleteCallback: GenericDeleteMutationCallback)
  extends KafkaMutationAvroConsumer[mypipe.avro.InsertMutation, mypipe.avro.UpdateMutation, mypipe.avro.DeleteMutation](
    topic = topic,
    zkConnect = zkConnect,
    groupId = groupId,
    valueDecoder = new KafkaGenericAvroDecoder()
  )(
    insertCallback,
    updateCallback,
    deleteCallback,
    implicitly[TypeTag[InsertMutation]],
    implicitly[TypeTag[UpdateMutation]],
    implicitly[TypeTag[DeleteMutation]]
  )

