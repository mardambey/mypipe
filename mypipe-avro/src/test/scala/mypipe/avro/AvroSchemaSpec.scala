package mypipe.avro

import mypipe.UnitSpec
import java.util.Properties
import mypipe.avro.schema.{ GenericSchemaRepository, ShortSchemaId, AvroSchema }
import org.apache.avro.Schema
import org.apache.avro.Schema.Parser
import org.scalatest.BeforeAndAfterAll
import org.schemarepo.InMemoryRepository
import org.schemarepo.config.Config
import org.schemarepo.server.RepositoryServer

class AvroSchemaSpec extends UnitSpec with BeforeAndAfterAll {

  val repoHost = "localhost"
  val repoPort = 8123
  val url = s"http://$repoHost:$repoPort/schema-repo";
  val topic = "mypipe_avro_test_user"
  var server: RepositoryServer = _

  // load up multiple versions of a schema
  val schemas = Array("/User-1-insert.avsc", "/User-2-insert.avsc", "/User-3-insert.avsc").map(
    schema ⇒ try {
      Some(new Parser().parse(getClass.getResourceAsStream(schema)))
    } catch {
      case e: Exception ⇒ None
    })

  val client1 = createSchemaRepoClient(url)
  val client2 = createSchemaRepoClient(url)
  val client3 = createSchemaRepoClient(url)

  override def beforeAll() {
    server = createInMemoryRepo(repoHost, repoPort)
  }

  override def afterAll() {
    server.stop
  }

  /** Create and start an InMemoryRepository.
   *
   *  @param host to listen on
   *  @param port to bind to
   *  @return the started RepositoryServer instance
   */
  protected def createInMemoryRepo(host: String, port: Int): RepositoryServer = {

    val props = new Properties()
    props.put(Config.REPO_CLASS, classOf[InMemoryRepository].getName)
    props.put(Config.JETTY_HOST, host)
    props.put(Config.JETTY_PORT, port.toString)

    val server = new RepositoryServer(props)
    server.start()
    server
  }

  def testRegiserAndFetch(client: GenericSchemaRepository[Short, Schema], topic: String, schema: Schema) {
    // register
    val id = client.registerSchema(topic, schema)

    // ask for schema, use cache
    assert(schema == client.getLatestSchema(topic).get)
    assert(schema == client.getSchema(topic, id).get)
    assert(id == client.getSchemaId(topic, schema).get)

    // ask for schema, flush cache
    assert(schema == client.getLatestSchema(topic, flushCache = true).get)
    assert(schema == client.getSchema(topic, id).get)
    assert(id == client.getSchemaId(topic, schema).get)
  }

  /** Create an Avro schema repo client.
   *  @param url of the schema repo
   *  @return the schema repo client
   */
  protected def createSchemaRepoClient(url: String) =
    new GenericSchemaRepository[Short, Schema]() with ShortSchemaId with AvroSchema { def getRepositoryURL = url }

  "An Avro schema repository client" should "register, fetch, and cache schemas" in {
    testRegiserAndFetch(client1, topic, schemas(0).get)
  }

  it should "register, fetch and cache new versions of schemas" in {
    testRegiserAndFetch(client2, topic, schemas(1).get)
    testRegiserAndFetch(client3, topic, schemas(2).get)
  }

  it should "fetch the latest schema ignoring it's cache and updating it" in {
    assert(client1.getLatestSchema(topic, flushCache = true).get == schemas(2).get)
  }

}
