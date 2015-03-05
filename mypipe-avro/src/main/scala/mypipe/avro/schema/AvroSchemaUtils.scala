package mypipe.avro.schema

import mypipe.api.event.Mutation
import org.apache.avro.Schema
import org.apache.avro.specific.SpecificRecord

object AvroSchemaUtils {

  def registerSchema(schemaRepoUrl: String, schemaStrId: String, avroName: String): Boolean = {

    val avroInstance: Option[SpecificRecord] = try {
      val clazz = Class.forName(avroName).asInstanceOf[Class[SpecificRecord]]

      Some(clazz.newInstance())
    } catch {
      case e: ClassNotFoundException ⇒ {
        println(s"The avro_name ($avroName) you passed in is not available in the JVM's classpath.")

        None
      }
      case e: Exception ⇒ {
        println(s"An unexpected exception occurred while trying to instantiate an instance of the avro_name ${avroName}:")
        println(s"${e.getMessage}: ${e.getStackTraceString}")

        None
      }
    }

    if (avroInstance.isEmpty) {
      println(s"Can't get an instance of avro_name ${avroName}")

      false
    } else {
      object TempSchemaRepoClient extends GenericSchemaRepository[Short, Schema] with ShortSchemaId with AvroSchema {
        def getRepositoryURL: String = schemaRepoUrl
      }

      val schemaId: Short = TempSchemaRepoClient.registerSchema(schemaStrId, avroInstance.get.getSchema)

      println(s"The Schema Repo has registered your schema under ID: $schemaId")

      val registeredSchema: Option[Schema] = TempSchemaRepoClient.getSchema(schemaStrId, schemaId)

      registeredSchema match {
        case Some(schema) ⇒ {
          println(s"The schema Repo contains the following schema for topic '$schemaStrId' ID $schemaId:")
          println(s"${schema.toString(true)}")

          true
        }
        case None ⇒ {
          println("We were not able to retrieve the registered schema from the repo!! Something probably went wrong....")

          false
        }
      }
    }
  }

  def genericSubject(mutation: Mutation): String =
    genericSubject(Mutation.typeAsString(mutation))

  def genericSubject(mtype: String): String =
    s"generic_${mtype}"

  /** Given a mutation returns an Avro schema repository subject based
   *  specifically on the mutation's database, table, and type (insert, update, delete).
   *
   *  @param mutation
   *  @return returns "mutationDbName_mutationTableName_mutationType" where mutationType is "insert", "update", or "delete"
   */
  def specificSubject(mutation: Mutation): String =
    specificSubject(mutation.table.db, mutation.table.name, Mutation.typeAsString(mutation))

  def specificSubject(db: String, table: String, mtype: String): String =
    s"${db}_${table}_${mtype}"
}
