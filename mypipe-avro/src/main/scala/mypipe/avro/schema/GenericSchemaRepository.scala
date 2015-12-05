package mypipe.avro.schema

import scala.collection.mutable
import java.util.logging.Logger
import com.google.common.collect.{ HashBiMap, BiMap }
import org.schemarepo.client.RESTRepositoryClient
import org.schemarepo.json.GsonJsonUtil
import org.schemarepo.{ BaseRepository, Subject, SchemaEntry }

trait SchemaRepository[ID, SCHEMA] {

  /** @param subject
   *  @param schemaId
   *  @return Some(schema) if the subject and schemaId are valid, None otherwise
   */
  def getSchema(subject: String, schemaId: ID): Option[SCHEMA]

  /** @param subject
   *  @return Some(schema) if the subject exists, None otherwise
   */
  def getLatestSchema(subject: String, flushCache: Boolean = false): Option[SCHEMA]

  /** @param subject
   *  @param schema
   *  @return Some(schemaId) if the subject and schema are valid, None otherwise
   */
  def getSchemaId(subject: String, schema: SCHEMA): Option[ID]

  /** @param subject
   *  @param schema
   *  @return schemaId, potentially an already existing one, if the schema isn't new.
   *  @throws Exception if registration is unsuccessful
   */
  def registerSchema(subject: String, schema: SCHEMA): ID
}

/** Generic implementation of a caching client for an AVRO-1124-style repo which provides strongly-typed APIs.
 *
 *  @tparam ID
 *  @tparam SCHEMA
 */
abstract class GenericSchemaRepository[ID, SCHEMA] extends SchemaRepository[ID, SCHEMA] {

  // Abstract functions which need to be overridden using traits or custom implementations.

  // Functions used to convert back and forth with the AVRO-1124 Schema Repo, which uses Strings for its IDs and Schemas.
  protected def idToString(id: ID): String
  protected def stringToId(id: String): ID
  protected def schemaToString(schema: SCHEMA): String
  protected def stringToSchema(schema: String): SCHEMA

  // Configuration
  protected def getRepositoryURL: String

  // Concrete implementation !

  // Utilities
  protected lazy val client: BaseRepository = new RESTRepositoryClient(getRepositoryURL, new GsonJsonUtil, true)
  private val logger = Logger.getLogger(classOf[GenericSchemaRepository[ID, SCHEMA]].getName)

  // Internal state
  private val idToSchemaCache = mutable.Map[String, BiMap[ID, SCHEMA]]()
  private val schemaToIdCache = mutable.Map[String, BiMap[SCHEMA, ID]]()
  private val latestSchemaCache = new java.util.HashMap[String, SCHEMA]()

  /** Utility function to DRY up the code.
   *
   *
   *  @param subject to look into to get the repository's corresponding [[org.apache.avro.repo.Subject]]
   *  @param key to store in the cache, if we are able to retrieve an entity
   *  @param map to store the key and entity into, if we are able to retrieve the entity
   *            N.B.: using a java.util.Map for compatibility with Guava's [[com.google.common.collect.BiMap]]
   *  @param entityRetrievalFunction to use on the [[org.apache.avro.repo.Subject]] to get a [[org.apache.avro.repo.SchemaEntry]]
   *  @param schemaEntryToStringFunction to use on the [[org.apache.avro.repo.SchemaEntry]] in order to get our (Stringly-typed) entity
   *  @param stringToValueFunction to convert the (Stringly-typed) entity into the proper type (VALUE).
   *  @param createMissingSubject to tell the function whether to create the subject in the remote repository, if it doesn't already exist (default = false).
   *  @param throwException to tell the function whether to throw an exception instead of returning None if there's any problem (default = false).
   *  @tparam KEY the type of the key in the map we want to update
   *  @tparam VALUE the type of the value in the map we want to update
   *  @return Some(schema) if the subject and key are valid, None otherwise
   */
  private def retrieveUnknownEntity[KEY, VALUE](subject: String,
                                                key: KEY,
                                                map: java.util.Map[KEY, VALUE],
                                                entityRetrievalFunction: Subject ⇒ SchemaEntry,
                                                schemaEntryToStringFunction: SchemaEntry ⇒ String,
                                                stringToValueFunction: String ⇒ VALUE,
                                                createMissingSubject: Boolean = false,
                                                throwException: Boolean = false): Option[VALUE] = {
    val subjectOption: Option[Subject] = client.lookup(subject) match {
      case null ⇒ {
        if (createMissingSubject) {
          Some(client.register(subject, null))
        } else {
          None
        }
      }
      case subject ⇒ Some(subject)
    }

    subjectOption match {
      case Some(subject) ⇒ {
        try {
          entityRetrievalFunction(subject) match {
            case null ⇒ {
              if (throwException) {
                throw new RuntimeException("An unknown problem occurred... the RESTRepositoryClient returned null.")
              } else {
                None
              }
            }
            case schemaEntry ⇒ {
              val value: VALUE = stringToValueFunction(
                schemaEntryToStringFunction(
                  schemaEntry))
              map.put(key, value)
              Some(value)
            }
          }
        } catch {
          case e: Exception ⇒ {
            logger.warning("Got an exception while trying to retrieve an entity from the RESTRepositoryClient!\n" +
              e.getMessage + ": " + e.getStackTraceString)

            if (throwException) {
              throw e
            } else {
              None
            }
          }
        }
      }
      case None ⇒ if (throwException) {
        throw new RuntimeException("The requested subject does not exist in the remote Schema Repository.")
      } else {
        None
      }
    }
  }

  private def retrieveEntity[KEY, VALUE](subject: String,
                                         key: KEY,
                                         mainCache: mutable.Map[String, BiMap[KEY, VALUE]],
                                         inverseCache: mutable.Map[String, BiMap[VALUE, KEY]],
                                         entityRetrievalFunction: Subject ⇒ SchemaEntry,
                                         schemaEntryToStringFunction: SchemaEntry ⇒ String,
                                         stringToValueFunction: String ⇒ VALUE,
                                         createMissingSubject: Boolean = false,
                                         flushCache: Boolean = false,
                                         throwException: Boolean = false): Option[VALUE] = {
      def specificRetrieveFunction(cachedMap: java.util.Map[KEY, VALUE]): Option[VALUE] = {
        retrieveUnknownEntity[KEY, VALUE](
          subject,
          key,
          cachedMap,
          entityRetrievalFunction,
          schemaEntryToStringFunction,
          stringToValueFunction,
          createMissingSubject,
          throwException)
      }

    mainCache.get(subject) match {
      case Some(existingCachedMap) ⇒ Option(existingCachedMap.get(key)) match {
        case None                       ⇒ specificRetrieveFunction(existingCachedMap)
        case someSchema if (flushCache) ⇒ specificRetrieveFunction(existingCachedMap)
        case someSchema                 ⇒ someSchema
      }
      case None ⇒ {
        val newMapToCache = HashBiMap.create[KEY, VALUE]()
        mainCache.put(subject, newMapToCache)
        inverseCache.put(subject, newMapToCache.inverse())
        specificRetrieveFunction(newMapToCache)
      }
    }
  }

  /** @param subject
   *  @param schemaId
   *  @return Some(schema) if the subject and schemaId are valid, None otherwise
   */
  def getSchema(subject: String, schemaId: ID): Option[SCHEMA] = {
    retrieveEntity[ID, SCHEMA](
      subject,
      schemaId,
      idToSchemaCache,
      schemaToIdCache,
      entityRetrievalFunction = _.lookupById(idToString(schemaId)),
      schemaEntryToStringFunction = _.getSchema,
      stringToValueFunction = stringToSchema)
  }

  /** @param subject
   *  @return Some(schema) if the subject exists, None otherwise
   */
  def getLatestSchema(subject: String, flushCache: Boolean = false): Option[SCHEMA] = {

      def retrieve = retrieveUnknownEntity[String, SCHEMA](
        subject,
        subject,
        latestSchemaCache,
        entityRetrievalFunction = _.latest,
        schemaEntryToStringFunction = _.getSchema,
        stringToValueFunction = stringToSchema)

    if (flushCache) {
      retrieve
    } else {
      Option(latestSchemaCache.get(subject)) match {
        case None       ⇒ retrieve
        case someSchema ⇒ someSchema
      }
    }
  }

  /** @param subject
   *  @param schema
   *  @return Some(schemaId) if the subject and schema are valid, None otherwise
   */
  def getSchemaId(subject: String, schema: SCHEMA): Option[ID] = {
    retrieveEntity[SCHEMA, ID](
      subject,
      schema,
      schemaToIdCache,
      idToSchemaCache,
      entityRetrievalFunction = _.lookupBySchema(schemaToString(schema)),
      schemaEntryToStringFunction = _.getId,
      stringToValueFunction = stringToId)
  }

  /** @param subject
   *  @param schema
   *  @return schemaId, potentially an already existing one, if the schema isn't new.
   *  @throws Exception if registration is unsuccessful
   */
  def registerSchema(subject: String, schema: SCHEMA): ID = {
    retrieveEntity[SCHEMA, ID](
      subject,
      schema,
      schemaToIdCache,
      idToSchemaCache,
      entityRetrievalFunction = _.register(schemaToString(schema)),
      schemaEntryToStringFunction = _.getId,
      stringToValueFunction = stringToId,
      createMissingSubject = true,
      throwException = true).get
  }
}