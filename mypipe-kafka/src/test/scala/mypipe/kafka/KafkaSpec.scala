package mypipe.kafka

import scala.reflect.runtime.universe.TypeTag
import mypipe.avro.GenericInMemorySchemaRepo
import mypipe.UnitSpec

class KafkaSpec extends UnitSpec {

  "A Kafka generic Avro consumer" should "consume inserts" in {

    val consumer = new TestKafkaAvroGenericConsumer("insert", "braindump:2181", "insert-test")({
      insertMutation ⇒
        {
          insertMutation
          true
        }
    })

    consumer.start

  }

}

class TestKafkaAvroGenericConsumer(topic: String, zkConnect: String, groupId: String)(callback: (mypipe.avro.InsertMutation) ⇒ Boolean)
  extends KafkaAvroRecordConsumer[mypipe.avro.InsertMutation](topic, zkConnect, groupId, GenericInMemorySchemaRepo)(callback)
