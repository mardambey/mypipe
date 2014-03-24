package mypipe.runner

import mypipe.mysql.{ BinlogConsumer, BinlogFilePos, HostPortUserPass }
import scala.collection.JavaConverters._
import mypipe.api.{ Log, Mapping, Producer }
import com.typesafe.config.ConfigFactory
import com.typesafe.config.Config
import mypipe.{ Conf, Pipe }

object PipeRunner extends App {

  import PipeRunnerUtil._

  val conf = ConfigFactory.load()

  lazy val producers: Map[String, Class[Producer]] = loadProducerClasses(conf, "mypipe.producers")
  lazy val consumers: Map[String, HostPortUserPass] = loadConsumerConfigs(conf, "mypipe.consumers")
  lazy val pipes: Seq[Pipe] = createPipes(conf, "mypipe.pipes", producers, consumers)

  if (pipes.isEmpty) {
    Log.info("No pipes defined, exiting.")
    sys.exit()
  }

  sys.addShutdownHook({
    Log.info("Shutting down...")
    pipes.foreach(p ⇒ p.disconnect())
  })

  Log.info(s"Connecting ${pipes.size} pipes...")
  pipes.foreach(_.connect())
}

object PipeRunnerUtil {

  def loadProducerClasses(conf: Config, key: String): Map[String, Class[Producer]] = {
    val PRODUCERS = conf.getObject("mypipe.producers").asScala
    PRODUCERS.map(kv ⇒ {
      val name = kv._1
      val prodConf = conf.getConfig(s"mypipe.producers.$name")
      val clazz = prodConf.getString("class")
      (name, Class.forName(clazz).asInstanceOf[Class[Producer]])
    }).toMap
  }

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
                  consumerConfigs: Map[String, HostPortUserPass]): Seq[Pipe] = {

    val PIPES = conf.getObject("mypipe.pipes").asScala

    PIPES.map(kv ⇒ {

      val name = kv._1

      Log.info(s"Loading configuration for $name pipe")

      val pipeConf = conf.getConfig(s"mypipe.pipes.$name")
      val enabled = if (pipeConf.hasPath("enabled")) pipeConf.getBoolean("enabled") else true

      if (enabled) {

        val consumers = pipeConf.getStringList("consumers").asScala
        val consumerInstances = consumers.map(c ⇒ createConsumer(pipeName = name, consumerConfigs(c))).toList

        // the following hack assumes a single producer per pipe
        // since we don't support multiple producers well when
        // tracking offsets (we'll track offsets for the entire
        // pipe and not per producer
        val producers = pipeConf.getObject("producer")
        val producerName = producers.entrySet().asScala.head.getKey()
        val producerConfig = pipeConf.getConfig(s"producer.${producerName}")
        val producerMappingClasses = if (producerConfig.hasPath("mappings")) producerConfig.getStringList("mappings").asScala else List[String]()
        val producerMappings = producerMappingClasses.map(m ⇒ Class.forName(m).newInstance()).toList.asInstanceOf[List[Mapping]]
        val producerInstance = createProducer(producerName, producerConfig, producerMappings, producerClasses(producerName))

        new Pipe(name, consumerInstances, producerInstance)

      } else {
        // disabled
        null
      }
    }).filter(_ != null).toSeq
  }

  protected def createConsumer(pipeName: String, params: HostPortUserPass): BinlogConsumer = {
    val filePos = Conf.binlogFilePos(params.host, params.port, pipeName).getOrElse(BinlogFilePos.current)
    val consumer = BinlogConsumer(params.host, params.port, params.user, params.password, filePos)
    consumer
  }

  protected def createProducer(id: String, config: Config, mappings: List[Mapping] = List.empty[Mapping], clazz: Class[Producer]): Producer = {
    try {
      val ctor = clazz.getConstructor(classOf[List[Mapping]], classOf[Config])

      if (ctor == null) throw new NullPointerException("Could not load ctor for class ${clazz}, aborting.")

      val producer = ctor.newInstance(mappings, config)
      producer
    } catch {
      case e: Exception ⇒ {
        Log.severe(s"Failed to configure producer $id: ${e.getMessage}\n${e.getStackTraceString}")
        null
      }
    }
  }
}
