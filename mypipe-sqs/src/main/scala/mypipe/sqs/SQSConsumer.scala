package mypipe.sqs

import org.slf4j.LoggerFactory
import scala.concurrent._
import ExecutionContext.Implicits.global
//import sqs.consumer.{ ConsumerConfig, Consumer ⇒ KConsumer, ConsumerConnector }
import awscala._, sqs._
import java.util.Properties
import scala.annotation.tailrec

/** Abstract class that consumes messages for the given topic from
 *  the SQS cluster running on the provided sqsQueue settings. The
 *  groupId is used to control if the consumer resumes from it's saved
 *  offset or not.
 *
 *  @param topic to read messages from
 *  @param sqsQueue containing the sqs brokers
 *  @param groupId used to identify the consumer
 */
abstract class SQSConsumer(topic: String, sqsQueue: String, groupId: String) {

  protected val log = LoggerFactory.getLogger(getClass.getName)
  protected var future: Future[Unit] = _
  @volatile protected var loop = true
  val iterator = new SQSIterator(topic, sqsQueue, groupId)

  /** Called every time a new message is pulled from
   *  the SQS topic.
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

class SQSIterator(topic: String, sqsQueue: String, groupId: String) {
  implicit val sqsRegion = SQS.at(Region.Oregon)
  val sqs = sqsRegion.createQueueAndReturnQueueName(sqsQueue)

  // protected val consumerConnector = createConsumerConnector(sqsQueue, groupId)
  // protected val mapStreams = consumerConnector.createMessageStreams(Map(topic -> 1))
  // protected val stream = mapStreams.get(topic).get.head
  // protected val consumerIterator = stream.iterator()

  // protected def createConsumerConnector(sqsQueue: String, groupId: String): ConsumerConnector = {
  //   //TODO
  //   val props = new Properties()
  //   props.put("zookeeper.connect", sqsQueue)
  //   props.put("group.id", groupId)
  //   props.put("zookeeper.session.timeout.ms", "400")
  //   props.put("zookeeper.sync.time.ms", "200")
  //   props.put("auto.commit.interval.ms", "1000")
  //   KConsumer.create(new ConsumerConfig(props))
  // }

  // def next(): Option[Array[Byte]] = Option(consumerIterator.next()).map(_.message())
  def next(): Option[Array[Byte]] = Option("Any String you want".getBytes)
  def stop(): Unit = false
}

// object Sub {
//   println("starting subscription service ..")
//   val s = new Subscriber(new SQSClient("localhost", 6379))
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