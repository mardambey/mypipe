package mypipe.kafka

import org.slf4j.LoggerFactory
import scala.concurrent._
import ExecutionContext.Implicits.global
import kafka.consumer.{ ConsumerConfig, Consumer ⇒ KConsumer, ConsumerConnector }
import java.util.Properties
import scala.annotation.tailrec

/** Abstract class that consumes messages for the given topic from
 *  the Kafka cluster running on the provided zkConnect settings. The
 *  groupId is used to controll if the consumer resumes from it's saved
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

  protected val consumerConnector = createConsumerConnector(zkConnect, groupId)
  protected val mapStreams = consumerConnector.createMessageStreams(Map(topic -> 1))
  protected val stream = mapStreams.get(topic).get.head
  protected val consumerIterator = stream.iterator()

  /** Called every time a new message is pulled from
   *  the Kafka topic.
   *
   *  @param bytes the message
   *  @return true to continue reading messages, false to stop
   */
  def onEvent(bytes: Array[Byte]): Boolean

  @tailrec private def consume {
    val message = consumerIterator.next()

    if (message != null) {
      val next = try {
        onEvent(message.message())
      } catch {
        case e: Exception ⇒ log.error("Failed deserializing or processing message."); false
      }

      if (next && loop) consume
    } else {
      Unit
    }
  }

  def start: Future[Unit] = {

    future = Future {
      try {
        consume
        stop
      } catch {
        case e: Exception ⇒ stop
      }
    }

    future
  }

  def stop {
    loop = false
    consumerConnector.shutdown()
  }

  protected def createConsumerConnector(zkConnect: String, groupId: String): ConsumerConnector = {
    val props = new Properties()
    props.put("zookeeper.connect", zkConnect)
    props.put("group.id", groupId)
    props.put("zookeeper.session.timeout.ms", "400")
    props.put("zookeeper.sync.time.ms", "200")
    props.put("auto.commit.interval.ms", "1000")
    KConsumer.create(new ConsumerConfig(props))
  }
}

