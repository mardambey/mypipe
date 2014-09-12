package mypipe.kafka

import mypipe._
import mypipe.api.Mutation
import mypipe.avro.{ AvroVersionedRecordDeserializer, InMemorySchemaRepo, GenericInMemorySchemaRepo }
import mypipe.avro.schema.{ AvroSchemaUtils, ShortSchemaId, AvroSchema, GenericSchemaRepository }
import mypipe.mysql.{ BinlogConsumer, BinlogFilePos }
import mypipe.producer.{ KafkaMutationSpecificAvroProducer, KafkaMutationGenericAvroProducer }
import org.apache.avro.Schema
import org.apache.avro.util.Utf8
import org.scalatest.BeforeAndAfterAll
import org.slf4j.LoggerFactory

import scala.concurrent.duration._
import scala.concurrent.{ Await, Future }

import scala.reflect.runtime.universe._

class KafkaSpecificSpec extends UnitSpec with DatabaseSpec with ActorSystemSpec with BeforeAndAfterAll {

  val log = LoggerFactory.getLogger(getClass)
  @volatile var connected = false
  @volatile var done = false
  val kafkaProducer = new KafkaMutationSpecificAvroProducer(
    conf.getConfig("mypipe.test.kafka-specific-producer"))

  val binlogConsumer = BinlogConsumer(hostname, port.toInt, username, password, BinlogFilePos.current)
  val pipe = new Pipe("test-pipe-kafka-specific", List(binlogConsumer), kafkaProducer)

  override def beforeAll() {

    db.connect
    pipe.connect()

    while (!db.connection.isConnected || !pipe.isConnected) { Thread.sleep(10) }

    Await.result(db.connection.sendQuery(Queries.CREATE.statement), 1 second)
    Await.result(db.connection.sendQuery(Queries.TRUNCATE.statement), 1 second)
  }

  override def afterAll() {
    pipe.disconnect()
    db.disconnect
  }

  "A specific Kafka Avro producer and consumer" should "properly produce and consume insert, update, and delete events" in withDatabase { db ⇒

    val DATABASE = "mypipe"
    val TABLE = "user"
    val USERNAME = "username"
    val LOGIN_COUNT = 5

    val kafkaConsumer = new KafkaMutationAvroConsumer[mypipe.kafka.UserInsert, mypipe.kafka.UserUpdate, mypipe.kafka.UserDelete, Short](
      topic = KafkaUtil.specificTopic(DATABASE, TABLE),
      zkConnect = "localhost:2181",
      groupId = s"mypipe_user_insert-${System.currentTimeMillis()}",
      schemaIdSizeInBytes = 2)(

      insertCallback = { insertMutation ⇒
        log.debug("consumed insert mutation: " + insertMutation)
        try {
          assert(insertMutation.getDatabase.toString == DATABASE)
          assert(insertMutation.getTable.toString == TABLE)
          assert(insertMutation.getUsername.toString == USERNAME)
          assert(insertMutation.getLoginCount == LOGIN_COUNT)
        }
        true
      },

      updateCallback = { updateMutation ⇒
        log.debug("consumed update mutation: " + updateMutation)
        try {
          assert(updateMutation.getDatabase.toString == DATABASE)
          assert(updateMutation.getTable.toString == TABLE)
          assert(updateMutation.getOldUsername.toString == USERNAME)
          assert(updateMutation.getNewUsername.toString == USERNAME + "2")
          assert(updateMutation.getOldLoginCount == LOGIN_COUNT)
          assert(updateMutation.getNewLoginCount == LOGIN_COUNT + 1)
        }
        true
      },

      deleteCallback = { deleteMutation ⇒
        log.debug("consumed delete mutation: " + deleteMutation)
        try {
          assert(deleteMutation.getDatabase.toString == DATABASE)
          assert(deleteMutation.getTable.toString == TABLE)
          assert(deleteMutation.getUsername.toString == USERNAME + "2")
          assert(deleteMutation.getLoginCount == LOGIN_COUNT + 1)
        }
        done = true
        true
      },

      implicitly[TypeTag[UserInsert]],
      implicitly[TypeTag[UserUpdate]],
      implicitly[TypeTag[UserDelete]]) {

      protected val schemaRepoClient: GenericSchemaRepository[Short, Schema] = TestSchemaRepo

      override def bytesToSchemaId(bytes: Array[Byte], offset: Int): Short = byteArray2Short(bytes, offset)
      private def byteArray2Short(data: Array[Byte], offset: Int) = (((data(offset) << 8)) | ((data(offset + 1) & 0xff))).toShort

      override protected def avroSchemaSubjectForMutationByte(byte: Byte): String = AvroSchemaUtils.specificSubject(DATABASE, TABLE, Mutation.byteToString(byte))

      override val insertDeserializer: AvroVersionedRecordDeserializer[UserInsert] = new AvroVersionedRecordDeserializer[UserInsert]()
      override val updateDeserializer: AvroVersionedRecordDeserializer[UserUpdate] = new AvroVersionedRecordDeserializer[UserUpdate]()
      override val deleteDeserializer: AvroVersionedRecordDeserializer[UserDelete] = new AvroVersionedRecordDeserializer[UserDelete]()
    }

    val future = kafkaConsumer.start

    Await.result(db.connection.sendQuery(Queries.INSERT.statement(loginCount = LOGIN_COUNT)), 2 seconds)
    Await.result(db.connection.sendQuery(Queries.UPDATE.statement), 2 seconds)
    Await.result(db.connection.sendQuery(Queries.DELETE.statement), 2 seconds)
    Await.result(Future { while (!done) Thread.sleep(100) }, 20 seconds)

    try {
      kafkaConsumer.stop
      Await.result(future, 5 seconds)
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