package mypipe.kafka

import scala.concurrent.{ ExecutionContext, Await }
import ExecutionContext.Implicits.global
import scala.reflect.runtime.universe.TypeTag
import scala.concurrent.duration._
import mypipe.avro.GenericInMemorySchemaRepo
import mypipe._
import java.util.concurrent.{ TimeUnit, LinkedBlockingQueue }
import mypipe.api._
import mypipe.producer.{ KafkaMutationGenericAvroProducer, QueueProducer }
import mypipe.mysql.{ BinlogFilePos, BinlogConsumer }
import scala.concurrent.Await
import mypipe.mysql.BinlogConsumer
import mypipe.mysql.BinlogConsumer
import mypipe.api.UpdateMutation
import mypipe.api.InsertMutation
import org.scalatest.BeforeAndAfterAll

class KafkaSpec extends UnitSpec with DatabaseSpec with ActorSystemSpec with BeforeAndAfterAll {

  @volatile var connected = false
  val kafkaProducer = new KafkaMutationGenericAvroProducer(
    List.empty[Mapping],
    conf.getConfig("mypipe.test.kafka-generic-producer"))

  val binlogConsumer = BinlogConsumer(hostname, port.toInt, username, password, BinlogFilePos.current)
  val pipe = new Pipe("test-pipe-kafka-generic", List(binlogConsumer), kafkaProducer)

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

  "A generic Kafka Avro producer and consumer" should "properly produce and consume insert events" in withDatabase { db ⇒

    val kafkaConsumer = new InsertKafkaAvroGenericConsumer("mypipe_user_insert", "localhost:2181", s"mypipe_user_insert-${System.currentTimeMillis()}")({
      insertMutation: mypipe.avro.InsertMutation ⇒
        {
          Log.info("consumed insert mutation: " + insertMutation)
          // TODO: don't hardcode this
          assert(insertMutation.getDatabase == "mypipe")
          assert(insertMutation.getTable == "user")
          assert(insertMutation.getStrings().get("username") == "username")
          false
        }
    })

    val future = kafkaConsumer.start

    db.connection.sendQuery(Queries.INSERT.statement)

    try { Await.result(future, 10 seconds) }
    catch { case e: Exception ⇒ try { kafkaConsumer.stop }; assert(false) }
  }

  it should "properly produce and consume update events" in withDatabase { db ⇒

    val kafkaConsumer = new UpdateKafkaAvroGenericConsumer("mypipe_user_update", "localhost:2181", s"mypipe_user_update-${System.currentTimeMillis()}")({
      updateMutation: mypipe.avro.UpdateMutation ⇒
        {
          Log.info("consumed update mutation: " + updateMutation)
          // TODO: don't hardcode this
          assert(updateMutation.getDatabase == "mypipe")
          assert(updateMutation.getTable == "user")
          assert(updateMutation.getOldStrings().get("username") == "username")
          assert(updateMutation.getNewStrings().get("username") == "username2")
          false
        }
    })

    val future = kafkaConsumer.start

    db.connection.sendQuery(Queries.UPDATE.statement)

    try { Await.result(future, 10 seconds) }
    catch { case e: Exception ⇒ try { kafkaConsumer.stop }; assert(false) }
  }

  it should "properly produce and consume delete events" in withDatabase { db ⇒

    val kafkaConsumer = new DeleteKafkaAvroGenericConsumer("mypipe_user_delete", "localhost:2181", s"mypipe_user_delete-${System.currentTimeMillis()}")({
      deleteMutation: mypipe.avro.DeleteMutation ⇒
        {
          Log.info("consumed delete mutation: " + deleteMutation)
          // TODO: don't hardcode this
          assert(deleteMutation.getDatabase == "mypipe")
          assert(deleteMutation.getTable == "user")
          assert(deleteMutation.getStrings().get("username") == "username2")
          false
        }
    })

    val future = kafkaConsumer.start

    db.connection.sendQuery(Queries.DELETE.statement)

    try { Await.result(future, 10 seconds) }
    catch { case e: Exception ⇒ try { kafkaConsumer.stop }; assert(false) }
  }
}

class InsertKafkaAvroGenericConsumer(
  topic: String, zkConnect: String, groupId: String)(callback: (mypipe.avro.InsertMutation) ⇒ Boolean)
    extends KafkaAvroRecordConsumer[mypipe.avro.InsertMutation](
      topic, zkConnect, groupId, GenericInMemorySchemaRepo)(callback)

class UpdateKafkaAvroGenericConsumer(
  topic: String, zkConnect: String, groupId: String)(callback: (mypipe.avro.UpdateMutation) ⇒ Boolean)
    extends KafkaAvroRecordConsumer[mypipe.avro.UpdateMutation](
      topic, zkConnect, groupId, GenericInMemorySchemaRepo)(callback)

class DeleteKafkaAvroGenericConsumer(
  topic: String, zkConnect: String, groupId: String)(callback: (mypipe.avro.DeleteMutation) ⇒ Boolean)
    extends KafkaAvroRecordConsumer[mypipe.avro.DeleteMutation](
      topic, zkConnect, groupId, GenericInMemorySchemaRepo)(callback)