package mypipe.runner

import mypipe.redis.consumer.GenericConsoleConsumer

object RedisGenericConsoleConsumer extends App {

  val topic: String = args(0)
  val redisConnect: String = args(1)
  val groupId: String = args(2)

  val consumer = new GenericConsoleConsumer(topic = topic, redisConnect = redisConnect, groupId = groupId)

  consumer.start

  sys.addShutdownHook({
    consumer.stop()
  })
}
