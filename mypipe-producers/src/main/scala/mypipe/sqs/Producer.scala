package mypipe.sqs

import awscala._, sqs._
import java.util.concurrent.LinkedBlockingQueue
import java.util
import java.util.logging.Logger
import mypipe.producer.MutationProducer

import scala.collection.JavaConversions._
import scala.util.control.NonFatal

class SQSProducer(sqsQueueName: String) extends MutationProducer {
  //TODO add error logging/handling
  val log = Logger.getLogger(getClass.getName)

  // TODO add connection error trapping/logging/retry
  implicit val sqs = SQS.at(Region.Oregon)
  val sqsQueue = sqs.createQueueAndReturnQueueName(sqsQueueName)

  val queue = new LinkedBlockingQueue[String]()

  def flush: Boolean = {
    val events = new util.ArrayList[String]()
    queue.drainTo(events, 10)

    while (events.length > 0) {
      try {
        sqsQueue.addAll(events)
        events.clear()
        queue.drainTo(events, 10)
        println("yes")
      } catch {
        case NonFatal(e) ⇒ {
          queue.addAll(events)
          events.clear()
          println("no")
        }
      }
    }

    true
  }

  def send(topic: String, jsonString: String) {
    queue.add(jsonString)
  }

  override def toString: String = {
    "SQSProducer"
  }

}
