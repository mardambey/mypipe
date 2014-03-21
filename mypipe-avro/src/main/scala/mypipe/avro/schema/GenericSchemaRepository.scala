package mypipe.avro.schema

import org.apache.avro.repo.client.RESTRepositoryClient
import scala.collection.mutable
import java.util.logging.Logger
import org.apache.avro.repo.{ SchemaEntry, Subject }

/** Generic implementation of a caching client for an AVRO-1124-style repo which provides strongly-typed APIs.
 *
 *  @tparam ID
 *  @tparam SCHEMA
 */
abstract class GenericSchemaRepository[ID, SCHEMA] {

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
  private lazy val client = new RESTRepositoryClient(getRepositoryURL)
  private val logger = Logger.getLogger(classOf[GenericSchemaRepository[ID, SCHEMA]].getName)

  // Internal state
  private val cache = mutable.Map[String, mutable.Map[ID, SCHEMA]]()
  private val latestSchemaCache = mutable.Map[String, SCHEMA]()

  /** Utility function to DRY up the getSchema and getLatestSchema code.
   *
   *  @param topic to look into
   *  @param key to store in the cache, if we are able to retrieve a schema
   *  @param map to store the key and Schema into, if we are able to retrieve a schema
   *  @param retrievalFunction to use on the SchemaEntry retrieved from the client in order to get our schema
   *  @tparam KEY the type of the key for the map we want to update
   *  @return Some(schema) if the topic and key are valid, None otherwise
   */
  private def retrieveUnknownSchema[KEY](topic: String,
                                         key: KEY,
                                         map: mutable.Map[KEY, SCHEMA],
                                         retrievalFunction: Subject ⇒ SchemaEntry): Option[SCHEMA] = {
    client.lookup(topic) match {
      case null ⇒ None
      case subject ⇒ retrievalFunction(subject) match {
        case null ⇒ None
        case schemaEntry ⇒ try {
          val schema: SCHEMA = stringToSchema(schemaEntry.getSchema)
          map.put(key, schema)
          Some(schema)
        } catch {
          case e: Exception ⇒ {
            logger.warning(s"Got an exception while parsing the schema received from the RESTRepositoryClient!\n" +
              s"${e.getMessage}:${e.getStackTraceString}")
            None
          }
        }
      }
    }
  }

  /** @param topic
   *  @param schemaId
   *  @return Some(schema) if the topic and schemaId are valid, None otherwise
   */
  def getSchema(topic: String, schemaId: ID): Option[SCHEMA] = {
      def retrieveUnknownSpecificSchema(topicMap: mutable.Map[ID, SCHEMA]): Option[SCHEMA] = {
        retrieveUnknownSchema[ID](topic, schemaId, topicMap, _.lookupById(idToString(schemaId)))
      }

    cache.get(topic) match {
      case Some(topicMap) ⇒ topicMap.get(schemaId) match {
        case None       ⇒ retrieveUnknownSpecificSchema(topicMap)
        case someSchema ⇒ someSchema
      }
      case None ⇒ {
        val topicMap = mutable.Map[ID, SCHEMA]()
        cache.put(topic, topicMap)
        retrieveUnknownSpecificSchema(topicMap)
      }
    }
  }

  /** @param topic
   *  @return Some(schema) if the topic exists, None otherwise
   */
  def getLatestSchema(topic: String): Option[SCHEMA] = {
      def retrieveUnknownLatestSchema: Option[SCHEMA] = {
        retrieveUnknownSchema[String](topic, topic, latestSchemaCache, _.latest)
      }

    latestSchemaCache.get(topic) match {
      case None       ⇒ retrieveUnknownLatestSchema
      case someSchema ⇒ someSchema
    }
  }

  /** @param topic
   *  @param schema
   *  @return Some(schemaId) if the topic and schema are valid, None otherwise
   */
  def getSchemaId(topic: String, schema: SCHEMA): Option[ID] = ???

  /** @param topic
   *  @param schema
   *  @return schemaId, potentially an already existing one, if the schema isn't new.
   *  @throws Exception if registration is unsuccessful
   */
  def registerSchema(topic: String, schema: SCHEMA): ID = ???
}