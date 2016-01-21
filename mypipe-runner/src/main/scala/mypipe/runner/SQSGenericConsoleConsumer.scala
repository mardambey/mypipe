package mypipe.runner

import mypipe.sqs.consumer.GenericConsoleConsumer

object SQSGenericConsoleConsumer extends App {

  val topic: String = args(0)
  val sqsQueue: String = args(1)
  val groupId: String = args(2)

  val consumer = new GenericConsoleConsumer(topic = topic, sqsQueue = sqsQueue, groupId = groupId)

  consumer.start

  sys.addShutdownHook({
    consumer.stop()
  })
}
