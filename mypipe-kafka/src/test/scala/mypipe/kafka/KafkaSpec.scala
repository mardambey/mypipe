package mypipe.kafka

import mypipe.UnitSpec

class KafkaSpec extends UnitSpec {


}

class TestKafkaAvroGenericConsumer(topic: String, zkConnect: String, groupId: String)
  extends KafkaAvroGenericConsumer(topic, zkConnect, groupId) {

}
