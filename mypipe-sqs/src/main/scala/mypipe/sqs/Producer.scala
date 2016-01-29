package mypipe.sqs

import java.util.Properties
import awscala._, sqs._
import java.util.concurrent.LinkedBlockingQueue
import java.util
import java.util.logging.Logger
import scala.collection.JavaConversions._

class SQSProducer(sqsQueueName: String) {
  //TODO add error logging/handling
  val log = Logger.getLogger(getClass.getName)

  // TODO add connection error trapping/logging/retry
  implicit val sqs = SQS.at(Region.Oregon)
  val sqsQueue = sqs.createQueueAndReturnQueueName(sqsQueueName)

  val queue = new LinkedBlockingQueue[String]()

  def flush: Boolean = {
    val events = new util.ArrayList[String]()
    queue.drainTo(events)

    if (events.length > 0) {
      sqsQueue.addAll(events)
    }

    true
  }

  def send(jsonString: String) {
    queue.add(jsonString)
  }

  override def toString: String = {
    "SQSProducer"
  }

}
