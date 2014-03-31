package mypipe.kafka

import mypipe.api._
import java.util.logging.Logger
import scala.concurrent._
import ExecutionContext.Implicits.global
import mypipe.avro.{ AvroVersionedSpecificRecordMutationDeserializer, AvroGenericRecordMutationDeserializer }
import kafka.consumer.{ ConsumerIterator, ConsumerConfig, Consumer, ConsumerConnector }
import java.util.Properties

abstract class Consumer[INPUT](topic: String) extends MutationDeserializer[INPUT] {

  val log = Logger.getLogger(getClass.getName)
  val iterator: Iterator[INPUT]
  var future: Future[Unit] = _
  @volatile var loop = true

  def start {

    future = Future {
      iterator.takeWhile(message ⇒ {
        val next = try { onEvent(deserialize(message)) } catch {
          case e: Exception ⇒ log.severe("Failed deserializing or processing message."); false
        }

        next && loop
      })
    }

    shutdown
  }

  def stop {
    loop = false
  }

  def onEvent(mutation: Mutation[_]): Boolean
  def shutdown

}

abstract class KafkaConsumer(topic: String, zkConnect: String, groupId: String)
    extends Consumer[Array[Byte]](topic) {

  def createConsumerConnector(zkConnect: String, groupId: String): ConsumerConnector = {
    val props = new Properties()
    props.put("zookeeper.connect", zkConnect)
    props.put("group.id", groupId)
    props.put("zookeeper.session.timeout.ms", "400")
    props.put("zookeeper.sync.time.ms", "200")
    props.put("auto.commit.interval.ms", "1000")
    Consumer.create(new ConsumerConfig(props))
  }

  def createIterator(iter: ConsumerIterator[Array[Byte], Array[Byte]]): Iterator[Array[Byte]] = new Iterator[Array[Byte]]() {
    override def next(): Array[Byte] = iter.next().message()
    override def hasNext: Boolean = iter.hasNext
  }

  val consumerConnector = createConsumerConnector(zkConnect, groupId)
  val mapStreams = consumerConnector.createMessageStreams(Map(topic -> 1))
  val stream = mapStreams.get(topic).get.head
  val consumerIterator = stream.iterator()
  val iterator: Iterator[Array[Byte]] = createIterator(consumerIterator)

  def shutdown {
    consumerConnector.shutdown()
  }

  def onEvent(mutation: Mutation[_]): Boolean = ???
}
abstract class KafkaAvroGenericConsumer(topic: String, zkConnect: String, groupId: String)
  extends KafkaConsumer(topic, zkConnect, groupId)
  with AvroGenericRecordMutationDeserializer

abstract class KafkaAvroVersionedSpecifcConsumer(topic: String, zkConnect: String, groupId: String)
  extends KafkaConsumer(topic, zkConnect, groupId)
  with AvroVersionedSpecificRecordMutationDeserializer

