package mypipe.runner

import mypipe.kafka.consumer.GenericConsoleConsumer

object KafkaGenericConsoleConsumer extends App {

  val topic: String = args(0)
  val zkConnect: String = args(1)
  val groupId: String = args(2)

  val consumer = new GenericConsoleConsumer(topic = topic, zkConnect = zkConnect, groupId = groupId)

  consumer.start

  sys.addShutdownHook({
    consumer.stop()
  })
}