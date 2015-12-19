package mypipe.kafka

import com.typesafe.config.ConfigFactory
import mypipe.Queries
import mypipe._
import mypipe.api.Conf

import mypipe.api.event.Mutation
import mypipe.api.repo.FileBasedBinaryLogPositionRepository
import mypipe.avro.AvroVersionedRecordDeserializer
import mypipe.avro.schema.AvroSchemaUtils
import mypipe.mysql.MySQLBinaryLogConsumer
import mypipe.pipe.Pipe
import mypipe.producer.KafkaMutationSpecificAvroProducer
import org.scalatest.BeforeAndAfterAll
import org.slf4j.LoggerFactory
import scala.concurrent.duration._

import scala.concurrent.Await

class KafkaAlterSpec extends UnitSpec with DatabaseSpec with ActorSystemSpec with BeforeAndAfterAll {

  val log = LoggerFactory.getLogger(getClass)

  @volatile var done = false

  val kafkaProducer = new KafkaMutationSpecificAvroProducer(
    conf.getConfig("mypipe.test.kafka-specific-producer"))

  val c = ConfigFactory.parseString(
    s"""
         |{
         |  source = "${Queries.DATABASE.host}:${Queries.DATABASE.port}:${Queries.DATABASE.username}:${Queries.DATABASE.password}"
         |}
         """.stripMargin)
  val binlogConsumer = MySQLBinaryLogConsumer(c)
  val binlogPosRepo = new FileBasedBinaryLogPositionRepository(filePrefix = "test-pipe-kafka-alter", dataDir = Conf.DATADIR)
  val pipe = new Pipe("test-pipe-kafka-alter", binlogConsumer, kafkaProducer, binlogPosRepo)

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

  "A Kafka producer " should "properly add and produce alter table added fields" in withDatabase { db ⇒

    val DATABASE = Queries.DATABASE.name
    val TABLE = Queries.TABLE.name
    val USERNAME = Queries.INSERT.username
    val USERNAME2 = Queries.UPDATE.username
    val LOGIN_COUNT = 5
    val schemaIdSizeInBytes = 2
    val headerLength = PROTO_HEADER_LEN_V0 + schemaIdSizeInBytes
    val zkConnect = conf.getString("mypipe.test.kafka-specific-producer.zk-connect")
    val topic = KafkaUtil.specificTopic(DATABASE, TABLE)
    val groupId = s"${DATABASE}_${TABLE}_specific_test-${System.currentTimeMillis()}"
    val iter = new KafkaIterator(topic, zkConnect, groupId)

      def byteArray2Short(data: Array[Byte], offset: Int) = ((data(offset) << 8) | (data(offset + 1) & 0xff)).toShort
      def avroSchemaSubjectForMutationByte(byte: Byte): String = AvroSchemaUtils.specificSubject(DATABASE, TABLE, Mutation.byteToString(byte))

    // insert into user
    Await.result(db.connection.sendQuery(Queries.INSERT.statement(loginCount = LOGIN_COUNT)), 2.seconds)
    // consume event from kafka
    iter.next()
    // add new schema to repo
    val newSchemaId = TestSchemaRepo.registerSchema(
      AvroSchemaUtils.specificSubject(DATABASE, TABLE, Mutation.InsertString),
      new UserInsert2().getSchema)
    // alter user
    Await.result(db.connection.sendQuery(Queries.ALTER.statementAdd), 2.seconds)
    // insert into user with new field
    Await.result(db.connection.sendQuery(Queries.INSERT.statement(loginCount = LOGIN_COUNT, email = Some("test@test.com"))), 2.seconds)
    // consume event with new field from kafka
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
