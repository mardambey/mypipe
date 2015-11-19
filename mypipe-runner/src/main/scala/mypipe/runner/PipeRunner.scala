package mypipe.runner

import mypipe.api.producer.Producer
import mypipe.mysql.{ MySQLBinaryLogConsumer, BinaryLogFilePosition }
import mypipe.pipe.Pipe

import scala.collection.JavaConverters._
import mypipe.api.{ Conf, HostPortUserPass }
import com.typesafe.config.ConfigFactory
import com.typesafe.config.Config
import org.slf4j.LoggerFactory

object PipeRunner extends App {

  import PipeRunnerUtil._

  protected val log = LoggerFactory.getLogger(getClass)
  protected val conf = ConfigFactory.load()

  lazy val producers: Map[String, Class[Producer]] = loadProducerClasses(conf, "mypipe.producers")
  lazy val consumers: Map[String, HostPortUserPass] = loadConsumerConfigs(conf, "mypipe.consumers")
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

  def loadProducerClasses(conf: Config, key: String): Map[String, Class[Producer]] =
    Conf.loadClassesForKey[Producer]("mypipe.producers")

  def loadConsumerConfigs(conf: Config, key: String): Map[String, HostPortUserPass] = {
    val CONSUMERS = conf.getObject("mypipe.consumers").asScala
    CONSUMERS.map(kv ⇒ {
      val name = kv._1
      val consConf = conf.getConfig(s"mypipe.consumers.$name")
      val source = consConf.getString("source")
      val params = HostPortUserPass(source)
      (name, params)
    }).toMap
  }

  def createPipes(conf: Config,
                  key: String,
                  producerClasses: Map[String, Class[Producer]],
                  consumerConfigs: Map[String, HostPortUserPass]): Seq[Pipe[_, _]] = {

    val pipes = conf.getObject("mypipe.pipes").asScala

    pipes.map(kv ⇒ {

      val name = kv._1

      log.info(s"Loading configuration for $name pipe")

      val pipeConf = conf.getConfig(s"mypipe.pipes.$name")
      val enabled = if (pipeConf.hasPath("enabled")) pipeConf.getBoolean("enabled") else true

      if (enabled) {

        val consumers = pipeConf.getStringList("consumers").asScala
        val consumerInstances = consumers.map(c ⇒ {
          val consumer = createConsumer(pipeName = name, consumerConfigs(c))
          val binlogFileAndPos = Conf.binlogLoadFilePosition(consumer.id, pipeName = name).getOrElse(BinaryLogFilePosition.current)
          consumer.setBinaryLogPosition(binlogFileAndPos)
          consumer
        }).toList

        // the following hack assumes a single producer per pipe
        // since we don't support multiple producers correctly when
        // tracking offsets (we'll track offsets for the entire
        // pipe and not per producer
        val producers = pipeConf.getObject("producer")
        val producerName = producers.entrySet().asScala.head.getKey
        val producerConfig = pipeConf.getConfig(s"producer.$producerName")
        val producerInstance = createProducer(producerName, producerConfig, producerClasses(producerName))

        new Pipe(name, consumerInstances, producerInstance)

      } else {
        // disabled
        null
      }
    }).filter(_ != null).toSeq
  }

  protected def createConsumer(pipeName: String, params: HostPortUserPass): MySQLBinaryLogConsumer = {
    val consumer = MySQLBinaryLogConsumer(params.host, params.port, params.user, params.password)
    consumer
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
