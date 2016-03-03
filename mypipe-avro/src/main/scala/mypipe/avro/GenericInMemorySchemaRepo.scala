package mypipe.avro

import mypipe.api.Conf
import mypipe.api.event.Mutation
import org.apache.avro.Schema
import mypipe.avro.schema.{ AvroSchemaUtils, AvroSchema, ShortSchemaId }

/** An in memory Avro schema repository that maps 3 topics: insert, update, delete.
 *  The schemas returned are InsertMutation, UpdateMutation, and DeleteMutation that
 *  allow for generic storage of insertions, updates, and deletes as Avro serialized
 *  data.
 *
 */
object GenericInMemorySchemaRepo extends InMemorySchemaRepo[Short, Schema] with ShortSchemaId with AvroSchema {
  val insertSchemaId = registerSchema(AvroSchemaUtils.genericSubject(Mutation.InsertString), if (Conf.INCLUDE_ROW_DATA) InsertMutation.getClassSchema else InsertMutationWithoutData.getClassSchema)
  val updateSchemaId = registerSchema(AvroSchemaUtils.genericSubject(Mutation.UpdateString), if (Conf.INCLUDE_ROW_DATA) UpdateMutation.getClassSchema else UpdateMutationWithoutData.getClassSchema)
  val deleteSchemaId = registerSchema(AvroSchemaUtils.genericSubject(Mutation.DeleteString), if (Conf.INCLUDE_ROW_DATA) DeleteMutation.getClassSchema else DeleteMutationWithoutData.getClassSchema)
}

