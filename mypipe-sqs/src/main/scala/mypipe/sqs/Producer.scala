package mypipe.sqs

import java.util.Properties
import awscala._, sqs._
import java.util.concurrent.LinkedBlockingQueue
import java.util
import java.util.logging.Logger
import scala.collection.JavaConversions._

class SQSProducer[MessageType](sqsQueue: String) {
  //TODO add error logging/handling
  val log = Logger.getLogger(getClass.getName)

  // TODO add connection error trapping/logging/retry
  implicit val sqsRegion = SQS.at(Region.Oregon)
  val sqs = sqsRegion.createQueueAndReturnQueueName(sqsQueue)

  val queue = new LinkedBlockingQueue[(String, String)]()

  def queue(topic: String, jsonString: String) {
    queue.add((topic, jsonString))
  }

  def flush: Boolean = {
    val events = new util.ArrayList[(String, String)]()
    queue.drainTo(events)

    events.toList.foreach(event ⇒ sqs.add(event._1 + " " + event._2))
    true
  }

  def send(topic: String, jsonString: String) {
    queue(topic, jsonString)
  }

  override def toString: String = {
    "SQSProducer"
  }

}
