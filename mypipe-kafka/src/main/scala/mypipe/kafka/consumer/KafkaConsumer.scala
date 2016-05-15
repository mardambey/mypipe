package mypipe.kafka.consumer

import kafka.serializer._
import org.slf4j.LoggerFactory

import scala.annotation.tailrec
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent._

/** Abstract class that consumes messages for the given topic from
 *  the Kafka cluster running on the provided zkConnect settings. The
 *  groupId is used to control if the consumer resumes from it's saved
 *  offset or not.
 *
 *  @param topic to read messages from
 *  @param zkConnect containing the kafka brokers
 *  @param groupId used to identify the consumer
 */
abstract class KafkaConsumer[T](topic: String, zkConnect: String, groupId: String, valueDecoder: Decoder[T]) {

  protected val log = LoggerFactory.getLogger(getClass.getName)
  protected var future: Future[Unit] = _
  @volatile protected var loop = true
  val iterator = new KafkaIterator(topic, zkConnect, groupId, valueDecoder)

  /** Called every time a new message is pulled from
   *  the Kafka topic.
   *
   *  @param data the data
   *  @return true to continue reading messages, false to stop
   */
  def onEvent(data: T): Boolean

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
        stop()
      } catch {
        case e: Exception ⇒ stop()
      }
    }

    future
  }

  def stop() {
    loop = false
    iterator.stop()
  }
}

