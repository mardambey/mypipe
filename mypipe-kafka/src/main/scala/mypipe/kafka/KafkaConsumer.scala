package mypipe.kafka

import org.slf4j.LoggerFactory
import scala.concurrent._
import ExecutionContext.Implicits.global
import kafka.consumer.{ ConsumerConfig, Consumer ⇒ KConsumer, ConsumerConnector }
import java.util.Properties
import scala.annotation.tailrec

/** Abstract class that consumes messages for the given topic from
 *  the Kafka cluster running on the provided zkConnect settings. The
 *  groupId is used to control if the consumer resumes from it's saved
 *  offset or not.
 *
 *  @param topic to read messages from
 *  @param zkConnect containing the kafka brokers
 *  @param groupId used to identify the consumer
 */
abstract class KafkaConsumer(topic: String, zkConnect: String, groupId: String) {

  protected val log = LoggerFactory.getLogger(getClass.getName)
  protected var future: Future[Unit] = _
  @volatile protected var loop = true
  val iterator = new KafkaIterator(topic, zkConnect, groupId)

  /** Called every time a new message is pulled from
   *  the Kafka topic.
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

class KafkaIterator(topic: String, zkConnect: String, groupId: String) {
  protected val consumerConnector = createConsumerConnector(zkConnect, groupId)
  protected val mapStreams = consumerConnector.createMessageStreams(Map(topic -> 1))
  protected val stream = mapStreams.get(topic).get.head
  protected val consumerIterator = stream.iterator()

  protected def createConsumerConnector(zkConnect: String, groupId: String): ConsumerConnector = {
    val props = new Properties()
    props.put("zookeeper.connect", zkConnect)
    props.put("group.id", groupId)
    props.put("zookeeper.session.timeout.ms", "400")
    props.put("zookeeper.sync.time.ms", "200")
    props.put("auto.commit.interval.ms", "1000")
    KConsumer.create(new ConsumerConfig(props))
  }

  def next(): Option[Array[Byte]] = Option(consumerIterator.next()).map(_.message())
  def stop(): Unit = consumerConnector.shutdown()
}
