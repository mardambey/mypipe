package mypipe.redis

import org.slf4j.LoggerFactory
import scala.concurrent._
import ExecutionContext.Implicits.global
//import redis.consumer.{ ConsumerConfig, Consumer ⇒ KConsumer, ConsumerConnector }
import com.redis.RedisClient
import java.util.Properties
import scala.annotation.tailrec

/** Abstract class that consumes messages for the given topic from
 *  the Redis cluster running on the provided redisConnect settings. The
 *  groupId is used to control if the consumer resumes from it's saved
 *  offset or not.
 *
 *  @param topic to read messages from
 *  @param redisConnect containing the redis brokers
 *  @param groupId used to identify the consumer
 */
abstract class RedisConsumer(topic: String, redisConnect: String, groupId: String) {

  protected val log = LoggerFactory.getLogger(getClass.getName)
  protected var future: Future[Unit] = _
  @volatile protected var loop = true
  val iterator = new RedisIterator(topic, redisConnect, groupId)

  /** Called every time a new message is pulled from
   *  the Redis topic.
   *
   *  @param bytes the message
   *  @return true to continue reading messages, false to stop
   */
  def onEvent(bytes: Array[Byte]): Boolean

  @tailrec private def consume(): Unit = {
    val message = iterator.next()

    if (message.isDefined) {
      val next = try { onEvent(message.get) }
      catch {
        case e: Exception ⇒ log.error(s"Failed deserializing or processing message. ${e.getMessage} at ${e.getStackTrace.mkString(sys.props("line.separator"))}"); false
      }

      if (next && loop) consume()
    } else {
      // do nothing
    }
  }

  def start: Future[Unit] = {

    future = Future {
      try {
        consume()
        stop
      } catch {
        case e: Exception ⇒ stop
      }
    }

    future
  }

  def stop {
    loop = false
    iterator.stop()
  }
}

class RedisIterator(topic: String, redisConnect: String, groupId: String) {
  val client = new RedisClient(redisConnect, 6379)

  // protected val consumerConnector = createConsumerConnector(redisConnect, groupId)
  // protected val mapStreams = consumerConnector.createMessageStreams(Map(topic -> 1))
  // protected val stream = mapStreams.get(topic).get.head
  // protected val consumerIterator = stream.iterator()

  // protected def createConsumerConnector(redisConnect: String, groupId: String): ConsumerConnector = {
  //   //TODO
  //   val props = new Properties()
  //   props.put("zookeeper.connect", redisConnect)
  //   props.put("group.id", groupId)
  //   props.put("zookeeper.session.timeout.ms", "400")
  //   props.put("zookeeper.sync.time.ms", "200")
  //   props.put("auto.commit.interval.ms", "1000")
  //   KConsumer.create(new ConsumerConfig(props))
  // }

  // def next(): Option[Array[Byte]] = Option(consumerIterator.next()).map(_.message())
  def next(): Option[Array[Byte]] = Option("Any String you want".getBytes)
  def stop(): Unit = client.disconnect
}

// object Sub {
//   println("starting subscription service ..")
//   val s = new Subscriber(new RedisClient("localhost", 6379))
//   s.start
//   s ! Register(callback)

//   def sub(channels: String*) = {
//     s ! Subscribe(channels.toArray)
//   }

//   def unsub(channels: String*) = {
//     s ! Unsubscribe(channels.toArray)
//   }

//   def callback(pubsub: PubSubMessage) = pubsub match {
//     //..
//   }
// }