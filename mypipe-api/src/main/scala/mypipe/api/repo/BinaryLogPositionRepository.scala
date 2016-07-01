package mypipe.api.repo

import com.typesafe.config.Config
import mypipe.api.consumer.BinaryLogConsumer
import mypipe.mysql.BinaryLogFilePosition
import mypipe.api.Conf.RichConfig

trait BinaryLogPositionRepository {
  def saveBinaryLogPosition(consumer: BinaryLogConsumer[_]): Unit
  def loadBinaryLogPosition(consumer: BinaryLogConsumer[_]): Option[BinaryLogFilePosition]
}

trait ConfigurableBinaryLogPositionRepository extends BinaryLogPositionRepository {
  val config: Config
}

/** Requires a Typesafe Config instance with the following format:
 *  {
 *   class = "package.class.that.extends.ConfigurableBinaryLogPositionRepository"
 *   config {
 *     config-key-1 = "value1"
 *     config-key-2 = "value2"
 *     ...
 *   }
 *  }
 *
 *  @param config
 */
class BinaryLogPositionRepositoryFromConfiguration(override val config: Config)
    extends ConfigurableBinaryLogPositionRepository {

  val handler: Option[ConfigurableBinaryLogPositionRepository] =
    config.getOptionalString("class")
      .map(
        Class.forName(_)
        .getConstructor(classOf[Config])
        .newInstance(config.getConfig("config"))
        .asInstanceOf[ConfigurableBinaryLogPositionRepository]
      )

  override def saveBinaryLogPosition(consumer: BinaryLogConsumer[_]): Unit = handler.foreach(_.saveBinaryLogPosition(consumer))
  override def loadBinaryLogPosition(consumer: BinaryLogConsumer[_]): Option[BinaryLogFilePosition] = handler.flatMap(_.loadBinaryLogPosition(consumer))

}
