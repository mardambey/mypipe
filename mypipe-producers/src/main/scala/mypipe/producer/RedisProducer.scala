package mypipe.producer

import java.util
import java.util.concurrent.LinkedBlockingQueue
import java.util.logging.Logger

import com.redis.RedisClient

import scala.collection.JavaConversions._

class RedisProducer(redisConnect: String) extends ProviderProducer {
  //TODO add error logging/handling
  val log = Logger.getLogger(getClass.getName)

  val client = new RedisClient(redisConnect, 6379)

  val queue = new LinkedBlockingQueue[(String, String)]()

  def flush: Boolean = {
    val events = new util.ArrayList[(String, String)]()
    queue.drainTo(events)

    events.toList.foreach(event ⇒ client.publish(event._1, event._2))
    true
  }

  def send(topic: String, jsonString: String) {
    queue.add((topic, jsonString))
  }

  override def toString: String = {
    "RedisProducer"
  }

}
