package mypipe.redis

import mypipe.Queries
import mypipe._

import mypipe.api.event.Mutation
import mypipe.avro.AvroVersionedRecordDeserializer
import mypipe.avro.schema.AvroSchemaUtils
import mypipe.mysql.MySQLBinaryLogConsumer
import mypipe.pipe.Pipe
import mypipe.producer.RedisMutationSpecificAvroProducer
import org.scalatest.BeforeAndAfterAll
import org.slf4j.LoggerFactory
import scala.concurrent.duration._

import scala.concurrent.Await

class RedisAlterSpec extends UnitSpec with DatabaseSpec with ActorSystemSpec with BeforeAndAfterAll {

  val log = LoggerFactory.getLogger(getClass)

  @volatile var done = false

  val redisProducer = new RedisMutationSpecificAvroProducer(
    conf.getConfig("mypipe.test.redis-specific-producer"))

  val binlogConsumer = MySQLBinaryLogConsumer(Queries.DATABASE.host, Queries.DATABASE.port, Queries.DATABASE.username, Queries.DATABASE.password)
  val pipe = new Pipe("test-pipe-redis-alter", List(binlogConsumer), redisProducer)

  override def beforeAll() {
    pipe.connect()
    super.beforeAll()
    while (!pipe.isConnected) {
      Thread.sleep(10)
    }
  }

  override def afterAll() {
    pipe.disconnect()
    super.afterAll()
  }

  "A Redis producer " should "properly add and produce alter table added fields" in withDatabase { db ⇒

    val DATABASE = Queries.DATABASE.name
    val TABLE = Queries.TABLE.name
    val USERNAME = Queries.INSERT.username
    val USERNAME2 = Queries.UPDATE.username
    val LOGIN_COUNT = 5
    val schemaIdSizeInBytes = 2
    val headerLength = PROTO_HEADER_LEN_V0 + schemaIdSizeInBytes
    val redisConnect = conf.getString("mypipe.test.redis-specific-producer.redis-connect")
    val topic = RedisUtil.specificTopic(DATABASE, TABLE)
    val groupId = s"${DATABASE}_${TABLE}_specific_test-${System.currentTimeMillis()}"
    val iter = new RedisIterator(topic, redisConnect, groupId)

      def byteArray2Short(data: Array[Byte], offset: Int) = ((data(offset) << 8) | (data(offset + 1) & 0xff)).toShort
      def avroSchemaSubjectForMutationByte(byte: Byte): String = AvroSchemaUtils.specificSubject(DATABASE, TABLE, Mutation.byteToString(byte))

    // insert into user
    Await.result(db.connection.sendQuery(Queries.INSERT.statement(loginCount = LOGIN_COUNT)), 2.seconds)
    // TODO redisify
    // consume event from redis
    iter.next()
    // add new schema to repo
    val newSchemaId = TestSchemaRepo.registerSchema(
      AvroSchemaUtils.specificSubject(DATABASE, TABLE, Mutation.InsertString),
      new UserInsert2().getSchema)
    // alter user
    Await.result(db.connection.sendQuery(Queries.ALTER.statementAdd), 2.seconds)
    // insert into user with new field
    Await.result(db.connection.sendQuery(Queries.INSERT.statement(loginCount = LOGIN_COUNT, email = Some("test@test.com"))), 2.seconds)
    // consume event with new field from
    val bytes = iter.next().get

    val magicByte = bytes(0)

    if (magicByte != PROTO_MAGIC_V0) {
      log.error(s"We have encountered an unknown magic byte! Magic Byte: $magicByte")
      assert(false)
    } else {
      val schemaId = byteArray2Short(bytes, PROTO_HEADER_LEN_V0)
      val schema = TestSchemaRepo
        .getSchema(avroSchemaSubjectForMutationByte(Mutation.InsertByte), schemaId)
      val data = new AvroVersionedRecordDeserializer[UserInsert2]().deserialize(schema.get, bytes, headerLength)
      assert(data.get.getEmail.toString.equals("test@test.com"))
    }

    iter.stop()
  }
}
