package mypipe.kafka

import com.typesafe.config.ConfigFactory
import mypipe.api.Conf
import mypipe.api.event.Mutation
import mypipe.api.repo.FileBasedBinaryLogPositionRepository
import mypipe.pipe.Pipe

import scala.concurrent.duration._
import mypipe._
import mypipe.producer.KafkaMutationGenericAvroProducer
import scala.concurrent.{ Future, Await }
import mypipe.mysql.MySQLBinaryLogConsumer
import org.scalatest.BeforeAndAfterAll
import org.slf4j.LoggerFactory
import org.apache.avro.util.Utf8
import mypipe.avro.GenericInMemorySchemaRepo
import mypipe.avro.schema.{ AvroSchemaUtils, GenericSchemaRepository }
import org.apache.avro.Schema

class KafkaGenericSpec extends UnitSpec with DatabaseSpec with ActorSystemSpec with BeforeAndAfterAll {

  val log = LoggerFactory.getLogger(getClass)

  @volatile var done = false

  val kafkaProducer = new KafkaMutationGenericAvroProducer(conf.getConfig("mypipe.test.kafka-generic-producer"))

  val c = ConfigFactory.parseString(
    s"""
         |{
         |  source = "${Queries.DATABASE.host}:${Queries.DATABASE.port}:${Queries.DATABASE.username}:${Queries.DATABASE.password}"
         |}
         """.stripMargin)
  val binlogConsumer = MySQLBinaryLogConsumer(c)
  val binlogPosRepo = new FileBasedBinaryLogPositionRepository(filePrefix = "test-pipe-kafka-generic", dataDir = Conf.DATADIR)
  val pipe = new Pipe("test-pipe-kafka-generic", binlogConsumer, kafkaProducer, binlogPosRepo)

  override def beforeAll() {
    pipe.connect()
    super.beforeAll()
    while (!pipe.isConnected) { Thread.sleep(10) }
  }

  override def afterAll() {
    pipe.disconnect()
    super.afterAll()
  }

  "A generic Kafka Avro producer and consumer" should "properly produce and consume insert, update, and delete events" in withDatabase { db ⇒

    val username = new Utf8("username")
    val zkConnect = conf.getString("mypipe.test.kafka-generic-producer.zk-connect")

    val kafkaConsumer = new KafkaGenericMutationAvroConsumer[Short](
      topic = KafkaUtil.genericTopic(Queries.DATABASE.name, Queries.TABLE.name),
      zkConnect = zkConnect,
      groupId = s"${Queries.DATABASE.name}_${Queries.TABLE.name}-${System.currentTimeMillis()}",
      schemaIdSizeInBytes = 2)(

      insertCallback = { insertMutation ⇒
        log.debug("consumed insert mutation: " + insertMutation)
        try {
          assert(insertMutation.getDatabase.toString == Queries.DATABASE.name)
          assert(insertMutation.getTable.toString == Queries.TABLE.name)
          assert(insertMutation.getStrings.get(username).toString.equals(Queries.INSERT.username))
        } catch {
          case e: Exception ⇒ log.error(s"Failed testing insert: ${e.getMessage}: ${e.getStackTrace.mkString(System.lineSeparator())}")
        }

        true
      },

      updateCallback = { updateMutation ⇒
        log.debug("consumed update mutation: " + updateMutation)
        try {
          assert(updateMutation.getDatabase.toString == Queries.DATABASE.name)
          assert(updateMutation.getTable.toString == Queries.TABLE.name)
          assert(updateMutation.getOldStrings.get(username).toString == Queries.INSERT.username)
          assert(updateMutation.getNewStrings.get(username).toString == Queries.UPDATE.username)
        } catch {
          case e: Exception ⇒ log.error(s"Failed testing update: ${e.getMessage}: ${e.getStackTrace.mkString(System.lineSeparator())}")
        }

        true
      },

      deleteCallback = { deleteMutation ⇒
        log.debug("consumed delete mutation: " + deleteMutation)
        try {
          assert(deleteMutation.getDatabase.toString == Queries.DATABASE.name)
          assert(deleteMutation.getTable.toString == Queries.TABLE.name)
          assert(deleteMutation.getStrings.get(username).toString == Queries.UPDATE.username)
        } catch {
          case e: Exception ⇒ log.error(s"Failed testing delete: ${e.getMessage}: ${e.getStackTrace.mkString(System.lineSeparator())}")
        }

        done = true
        true

      }) {

      protected val schemaRepoClient: GenericSchemaRepository[Short, Schema] = GenericInMemorySchemaRepo
      override def bytesToSchemaId(bytes: Array[Byte], offset: Int): Short = byteArray2Short(bytes, offset)
      private def byteArray2Short(data: Array[Byte], offset: Int) = ((data(offset) << 8) | (data(offset + 1) & 0xff)).toShort

      override protected def avroSchemaSubjectForMutationByte(byte: Byte): String = AvroSchemaUtils.genericSubject(Mutation.byteToString(byte))
    }

    val future = kafkaConsumer.start

    Await.result(db.connection.sendQuery(Queries.INSERT.statement), 2.seconds)
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
