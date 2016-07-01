package mypipe.kafka.consumer

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

class GenericConsoleConsumer(topic: String, zkConnect: String, groupId: String) {

  val timeout = 10.seconds
  var future: Option[Future[Unit]] = None

  val kafkaConsumer = new KafkaGenericMutationAvroConsumer(
    topic = topic,
    zkConnect = zkConnect,
    groupId = groupId
  )(

    insertCallback = { insertMutation ⇒
    println(insertMutation)
    true
  },

    updateCallback = { updateMutation ⇒
    println(updateMutation)
    true
  },

    deleteCallback = { deleteMutation ⇒
    println(deleteMutation)
    true
  }
  ) {
  }

  def start(): Unit = {
    future = Some(kafkaConsumer.start)
  }

  def stop(): Unit = {

    kafkaConsumer.stop()
    future.foreach(Await.result(_, timeout))
  }
}
