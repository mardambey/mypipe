package mypipe.kafka

import com.typesafe.config.ConfigFactory
import mypipe._

import mypipe.api.event.Mutation
import mypipe.avro.{ AvroVersionedRecordDeserializer, InMemorySchemaRepo }
import mypipe.avro.schema.{ AvroSchemaUtils, ShortSchemaId, AvroSchema, GenericSchemaRepository }
import mypipe.mysql.MySQLBinaryLogConsumer
import mypipe.pipe.Pipe
import mypipe.producer.KafkaMutationSpecificAvroProducer
import org.apache.avro.Schema
import org.scalatest.BeforeAndAfterAll
import org.slf4j.LoggerFactory

import scala.concurrent.duration._
import scala.concurrent.{ Await, Future }

import scala.reflect.runtime.universe._

class KafkaSpecificSpec extends UnitSpec with DatabaseSpec with ActorSystemSpec with BeforeAndAfterAll {

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
  val pipe = new Pipe("test-pipe-kafka-specific", binlogConsumer, kafkaProducer)

  override def beforeAll() {
    pipe.connect()
    super.beforeAll()
    while (!pipe.isConnected) { Thread.sleep(10) }
  }

  override def afterAll() {
    pipe.disconnect()
    super.afterAll()
  }

  "A specific Kafka Avro producer and consumer" should "properly produce and consume insert, update, and delete events" in withDatabase { db ⇒

    val DATABASE = Queries.DATABASE.name
    val TABLE = Queries.TABLE.name
    val USERNAME = Queries.INSERT.username
    val USERNAME2 = Queries.UPDATE.username
    val LOGIN_COUNT = 5
    val zkConnect = conf.getString("mypipe.test.kafka-specific-producer.zk-connect")

    val kafkaConsumer = new KafkaMutationAvroConsumer[mypipe.kafka.UserInsert, mypipe.kafka.UserUpdate, mypipe.kafka.UserDelete, Short](
      topic = KafkaUtil.specificTopic(DATABASE, TABLE),
      zkConnect = zkConnect,
      groupId = s"${DATABASE}_${TABLE}_specific_test-${System.currentTimeMillis()}",
      schemaIdSizeInBytes = 2)(

      insertCallback = { insertMutation ⇒
        log.debug("consumed insert mutation: " + insertMutation)
        try {
          assert(insertMutation.getDatabase.toString == DATABASE)
          assert(insertMutation.getTable.toString == TABLE)
          assert(insertMutation.getUsername.toString == USERNAME)
          assert(insertMutation.getLoginCount == LOGIN_COUNT)
        } catch {
          case e: Exception ⇒ log.error(s"Failed testing insert: ${e.getMessage}: ${e.getStackTrace.mkString(System.lineSeparator())}")
        }

        true
      },

      updateCallback = { updateMutation ⇒
        log.debug("consumed update mutation: " + updateMutation)
        try {
          assert(updateMutation.getDatabase.toString == DATABASE)
          assert(updateMutation.getTable.toString == TABLE)
          assert(updateMutation.getOldUsername.toString == USERNAME)
          assert(updateMutation.getNewUsername.toString == USERNAME2)
          assert(updateMutation.getOldLoginCount == LOGIN_COUNT)
          assert(updateMutation.getNewLoginCount == LOGIN_COUNT + 1)
        } catch {
          case e: Exception ⇒ log.error(s"Failed testing update: ${e.getMessage}: ${e.getStackTrace.mkString(System.lineSeparator())}")
        }

        true
      },

      deleteCallback = { deleteMutation ⇒
        log.debug("consumed delete mutation: " + deleteMutation)
        try {
          assert(deleteMutation.getDatabase.toString == DATABASE)
          assert(deleteMutation.getTable.toString == TABLE)
          assert(deleteMutation.getUsername.toString == USERNAME2)
          assert(deleteMutation.getLoginCount == LOGIN_COUNT + 1)
        } catch {
          case e: Exception ⇒ log.error(s"Failed testing delete: ${e.getMessage}: ${e.getStackTrace.mkString(System.lineSeparator())}")
        }

        done = true
        true
      },

      implicitly[TypeTag[UserInsert]],
      implicitly[TypeTag[UserUpdate]],
      implicitly[TypeTag[UserDelete]]) {

      protected val schemaRepoClient: GenericSchemaRepository[Short, Schema] = TestSchemaRepo

      override def bytesToSchemaId(bytes: Array[Byte], offset: Int): Short = byteArray2Short(bytes, offset)
      private def byteArray2Short(data: Array[Byte], offset: Int) = ((data(offset) << 8) | (data(offset + 1) & 0xff)).toShort

      override protected def avroSchemaSubjectForMutationByte(byte: Byte): String = AvroSchemaUtils.specificSubject(DATABASE, TABLE, Mutation.byteToString(byte))

      override val insertDeserializer: AvroVersionedRecordDeserializer[UserInsert] = new AvroVersionedRecordDeserializer[UserInsert]()
      override val updateDeserializer: AvroVersionedRecordDeserializer[UserUpdate] = new AvroVersionedRecordDeserializer[UserUpdate]()
      override val deleteDeserializer: AvroVersionedRecordDeserializer[UserDelete] = new AvroVersionedRecordDeserializer[UserDelete]()
    }

    val future = kafkaConsumer.start

    Await.result(db.connection.sendQuery(Queries.INSERT.statement(loginCount = LOGIN_COUNT)), 2.seconds)
    Await.result(db.connection.sendQuery(Queries.UPDATE.statement), 2.seconds)
    Await.result(db.connection.sendQuery(Queries.DELETE.statement), 2.seconds)
    Await.result(Future { while (!done) Thread.sleep(100) }, 20.seconds)

    try {
      kafkaConsumer.stop
      Await.result(future, 5.seconds)
    } catch {
      case e: Exception ⇒ log.error(s"Failed stopping consumer: ${e.getMessage}: ${e.getStackTrace.mkString(System.lineSeparator())}")
    }

    if (!done) assert(false)
  }
}

object TestSchemaRepo extends InMemorySchemaRepo[Short, Schema] with ShortSchemaId with AvroSchema {
  val DATABASE = "mypipe"
  val TABLE = "user"
  val insertSchemaId = registerSchema(AvroSchemaUtils.specificSubject(DATABASE, TABLE, Mutation.InsertString), new UserInsert().getSchema)
  val updateSchemaId = registerSchema(AvroSchemaUtils.specificSubject(DATABASE, TABLE, Mutation.UpdateString), new UserUpdate().getSchema)
  val deleteSchemaId = registerSchema(AvroSchemaUtils.specificSubject(DATABASE, TABLE, Mutation.DeleteString), new UserDelete().getSchema)
}