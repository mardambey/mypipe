package mypipe.avro

import mypipe.api.event.Mutation
import org.apache.avro.Schema
import mypipe.avro.schema.{ AvroSchemaUtils, AvroSchema, ShortSchemaId }

/** An in memory Avro schema repository that maps 3 topics: insert, update, delete.
 *  The schemas returned are InsertMutation,UpdateMutation,DeleteMutation.avsc that
 *  allow for generic storage of insertions, updates, and deletes as Avro serialized
 *  data.
 *
 */
object GenericInMemorySchemaRepo extends InMemorySchemaRepo[Short, Schema] with ShortSchemaId with AvroSchema {
  // Avro schemas to be used when serializing mutations
  val insertSchemaFile: String = "/InsertMutation.avsc"
  val updateSchemaFile: String = "/UpdateMutation.avsc"
  val deleteSchemaFile: String = "/DeleteMutation.avsc"

  val insertSchema = try { Schema.parse(getClass.getResourceAsStream(insertSchemaFile)) } catch { case e: Exception ⇒ println("Failed on insert: " + e.getMessage); null }
  val updateSchema = try { Schema.parse(getClass.getResourceAsStream(updateSchemaFile)) } catch { case e: Exception ⇒ println("Failed on update: " + e.getMessage); null }
  val deleteSchema = try { Schema.parse(getClass.getResourceAsStream(deleteSchemaFile)) } catch { case e: Exception ⇒ println("Failed on delete: " + e.getMessage); null }

  val insertSchemaId = registerSchema(AvroSchemaUtils.genericSubject(Mutation.InsertString), insertSchema)
  val updateSchemaId = registerSchema(AvroSchemaUtils.genericSubject(Mutation.UpdateString), updateSchema)
  val deleteSchemaId = registerSchema(AvroSchemaUtils.genericSubject(Mutation.DeleteString), deleteSchema)
}

