package mypipe.api.repo

import java.util.Properties

import com.typesafe.config.Config
import mypipe.api.consumer.BinaryLogConsumer
import mypipe.mysql.BinaryLogFilePosition

class KafkaBasedBinaryLogPositionRepository(producerProps: Properties, consumerProps: Properties) extends BinaryLogPositionRepository {
  def saveBinaryLogPosition(consumer: BinaryLogConsumer[_]): Unit = ???
  def loadBinaryLogPosition(consumer: BinaryLogConsumer[_]): Option[BinaryLogFilePosition] = ???
}

object ConfigurableKafkaBasedBinaryLogPositionRepository {
  def producerProperties(config: Config): Properties = ???
  def consumerProperties(config: Config): Properties = ???
}

class ConfigurableKafkaBasedBinaryLogPositionRepository(override val config: Config)
  extends KafkaBasedBinaryLogPositionRepository(
    ConfigurableKafkaBasedBinaryLogPositionRepository.producerProperties(config),
    ConfigurableKafkaBasedBinaryLogPositionRepository.consumerProperties(config))
  with ConfigurableBinaryLogPositionRepository

