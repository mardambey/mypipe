package mypipe.kafka.consumer

import java.util.Properties

import kafka.consumer.{ ConsumerConfig, ConsumerConnector, Consumer ⇒ KConsumer }
import kafka.serializer.{ Decoder, DefaultDecoder }

class KafkaIterator[T](topic: String, zkConnect: String, groupId: String, valueDecoder: Decoder[T]) {
  protected val consumerConnector = createConsumerConnector(zkConnect, groupId)
  protected val mapStreams = consumerConnector.createMessageStreams(Map(topic -> 1), keyDecoder = new DefaultDecoder(), valueDecoder = valueDecoder)
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

  def next(): Option[T] = Option(consumerIterator.next()).map(_.message())
  def stop(): Unit = consumerConnector.shutdown()
}
