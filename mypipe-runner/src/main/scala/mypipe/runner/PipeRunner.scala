package mypipe.runner

import mypipe.api.consumer.BinaryLogConsumer
import mypipe.api.producer.Producer
import mypipe.mysql.{ MySQLBinaryLogConsumer, BinaryLogFilePosition }
import mypipe.pipe.Pipe

import scala.collection.JavaConverters._
import mypipe.api.Conf
import com.typesafe.config.ConfigFactory
import com.typesafe.config.Config
import org.slf4j.LoggerFactory

object PipeRunner extends App {

  import PipeRunnerUtil._

  protected val log = LoggerFactory.getLogger(getClass)
  protected val conf = ConfigFactory.load()

  lazy val producers: Map[String, Option[Class[Producer]]] = loadProducerClasses(conf, "mypipe.producers")
  lazy val consumers: Seq[(String, Config, Option[Class[BinaryLogConsumer[_, _]]])] = loadConsumerConfigs(conf, "mypipe.consumers")
  lazy val pipes: Seq[Pipe[_, _]] = createPipes(conf, "mypipe.pipes", producers, consumers)

  if (pipes.isEmpty) {
    log.info("No pipes defined, exiting.")
    sys.exit()
  }

  sys.addShutdownHook({
    log.info("Shutting down...")
    pipes.foreach(_.disconnect())
  })

  log.info(s"Connecting ${pipes.size} pipes...")
  pipes.foreach(_.connect())
}

object PipeRunnerUtil {

  protected val log = LoggerFactory.getLogger(getClass)

  def loadProducerClasses(conf: Config, key: String): Map[String, Option[Class[Producer]]] =
    Conf.loadClassesForKey[Producer](key)

  def loadConsumerClasses(conf: Config, key: String): Map[String, Option[Class[BinaryLogConsumer[_, _]]]] =
    Conf.loadClassesForKey[BinaryLogConsumer[_, _]](key)

  def loadConsumerConfigs(conf: Config, key: String): Seq[(String, Config, Option[Class[BinaryLogConsumer[_, _]]])] = {
    val consumerClasses = loadConsumerClasses(conf, key)
    val consumers = conf.getObject(key).asScala
    consumers.map(kv ⇒ {
      val name = kv._1
      val consConf = conf.getConfig(s"$key.$name")
      val clazz = consumerClasses.get(name).orElse(None)
      (name, consConf, clazz.getOrElse(None))
    }).toSeq
  }

  def createPipes(conf: Config,
                  key: String,
                  producerClasses: Map[String, Option[Class[Producer]]],
                  consumerConfigs: Seq[(String, Config, Option[Class[BinaryLogConsumer[_, _]]])]): Seq[Pipe[_, _]] = {

    val pipes = conf.getObject(key).asScala

    pipes.map(kv ⇒ {
      val name = kv._1
      val pipeConf = conf.getConfig(s"$key.$name")
      createPipe(name, pipeConf, producerClasses, consumerConfigs)
    }).filter(_ != null).toSeq
  }

  def createPipe(name: String, pipeConf: Config, producerClasses: Map[String, Option[Class[Producer]]], consumerConfigs: Seq[(String, Config, Option[Class[BinaryLogConsumer[_, _]]])]): Pipe[_, _] = {

    log.info(s"Loading configuration for $name pipe")

    val enabled = if (pipeConf.hasPath("enabled")) pipeConf.getBoolean("enabled") else true

    if (enabled) {

      val consumersConf = pipeConf.getStringList("consumers").asScala

      // config allows for multiple consumers, we only take the first one
      val consumerInstance = {
        val c = consumersConf.head
        val consumer = createConsumer(pipeName = name, consumerConfigs.head)

        // TODO: this is an ugly hack, make it generic
        if (consumer.isInstanceOf[MySQLBinaryLogConsumer]) {
          val binlogFileAndPos = Conf.binlogLoadFilePosition(consumer.id, pipeName = name).getOrElse(BinaryLogFilePosition.current)
          consumer.asInstanceOf[MySQLBinaryLogConsumer].setBinaryLogPosition(binlogFileAndPos)
        }

        consumer
      }

      // the following hack assumes a single producer per pipe
      // since we don't support multiple producers correctly when
      // tracking offsets (we'll track offsets for the entire
      // pipe and not per producer
      val producers = pipeConf.getObject("producer")
      val producerName = producers.entrySet().asScala.head.getKey
      val producerConfig = pipeConf.getConfig(s"producer.$producerName")
      // TODO: handle None
      val producerInstance = createProducer(producerName, producerConfig, producerClasses(producerName).get)

      new Pipe(name, consumerInstance, producerInstance)

    } else {
      // disabled
      null
    }
  }

  protected def createConsumer(pipeName: String, params: (String, Config, Option[Class[BinaryLogConsumer[_, _]]])): BinaryLogConsumer[_, _] = {
    try {
      val consumer = params._3 match {
        case None ⇒ MySQLBinaryLogConsumer(params._2).asInstanceOf[BinaryLogConsumer[_, _]]
        case Some(clazz) ⇒
          val consumer = {
            clazz.getConstructors.find(
              _.getParameterTypes.headOption.exists(_.equals(classOf[Config])))
              .map(_.newInstance(params._2))
              .getOrElse(clazz.newInstance())
          }

          //if (ctor == null) throw new NullPointerException(s"Could not load ctor for class $clazz, aborting.")

          // TODO: this is done specifically for the SelectConsumer for now, other consumers will fail since there is nothing forcing this ctor
          //val consumer = ctor.newInstance(params._2.user, params._2.host, params._2.password, new Integer(params._2.port))
          consumer.asInstanceOf[BinaryLogConsumer[_, _]]
      }

      consumer
    } catch {
      case e: Exception ⇒
        log.error(s"Failed to configure consumer ${params._1}: ${e.getMessage}\n${e.getStackTrace.mkString("\n")}")
        null
    }
  }

  protected def createProducer(id: String, config: Config, clazz: Class[Producer]): Producer = {
    try {
      val ctor = clazz.getConstructor(classOf[Config])

      if (ctor == null) throw new NullPointerException(s"Could not load ctor for class $clazz, aborting.")

      val producer = ctor.newInstance(config)
      producer
    } catch {
      case e: Exception ⇒
        log.error(s"Failed to configure producer $id: ${e.getMessage}\n${e.getStackTrace.mkString("\n")}")
        null
    }
  }
}
