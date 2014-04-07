package mypipe.kafka

import scala.reflect.runtime.universe._
import scala.concurrent.duration._
import mypipe.avro.GenericInMemorySchemaRepo
import mypipe._
import mypipe.api._
import mypipe.producer.KafkaMutationGenericAvroProducer
import mypipe.mysql.BinlogFilePos
import scala.concurrent.Await
import mypipe.mysql.BinlogConsumer
import org.scalatest.BeforeAndAfterAll
import org.apache.avro.specific.SpecificRecord

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

    val kafkaConsumer = new KafkaAvroGenericConsumer[mypipe.avro.InsertMutation]("mypipe_user_insert", "localhost:2181", s"mypipe_user_insert-${System.currentTimeMillis()}")({
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

    val kafkaConsumer = new KafkaAvroGenericConsumer[mypipe.avro.UpdateMutation]("mypipe_user_update", "localhost:2181", s"mypipe_user_update-${System.currentTimeMillis()}")({
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

    val kafkaConsumer = new KafkaAvroGenericConsumer[mypipe.avro.DeleteMutation]("mypipe_user_delete", "localhost:2181", s"mypipe_user_delete-${System.currentTimeMillis()}")({
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

class KafkaAvroGenericConsumer[InputRecord <: SpecificRecord](
  topic: String, zkConnect: String, groupId: String)(callback: (InputRecord) ⇒ Boolean)(implicit val tag: TypeTag[InputRecord])
    extends KafkaAvroRecordConsumer[InputRecord](
      topic, zkConnect, groupId, GenericInMemorySchemaRepo)(callback)

