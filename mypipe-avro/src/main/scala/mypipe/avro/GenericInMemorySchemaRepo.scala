package mypipe.avro

import org.apache.avro.Schema
import mypipe.avro.schema.{ AvroSchema, ShortSchemaId }

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

  val insertSchemaId = registerSchema("insert", insertSchema)
  val updateSchemaId = registerSchema("update", updateSchema)
  val deleteSchemaId = registerSchema("delete", deleteSchema)
}

