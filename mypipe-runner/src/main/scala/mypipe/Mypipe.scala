package mypipe

import mypipe.mysql.{ BinlogConsumer, BinlogFilePos, HostPortUserPass }
import scala.collection.JavaConverters._
import mypipe.api.{ Pipe, Log, Mapping, Producer }
import com.typesafe.config.Config

object Mypipe extends App {

  val producers: Map[String, Class[Producer]] = Conf.PRODUCERS.map(kv ⇒ {
    val name = kv._1
    val conf = Conf.conf.getConfig(s"mypipe.producers.$name")
    val clazz = conf.getString("class")
    (name, Class.forName(clazz).asInstanceOf[Class[Producer]])
  }).toMap

  val consumers: Map[String, HostPortUserPass] = Conf.CONSUMERS.map(kv ⇒ {
    val name = kv._1
    val conf = Conf.conf.getConfig(s"mypipe.consumers.$name")
    val source = conf.getString("source")
    val params = HostPortUserPass(source)
    (name, params)
  }).toMap

  val pipes: List[Pipe] = Conf.PIPES.map(kv ⇒ {

    val name = kv._1

    Log.info(s"Loading configuration for $name pipe")

    val conf = Conf.conf.getConfig(s"mypipe.pipes.$name")
    val enabled = if (conf.hasPath("enabled")) conf.getBoolean("enabled") else true

    if (enabled) {

      val consumers = conf.getStringList("consumers").asScala
      val consumerInstances = consumers.map(c ⇒ createConsumer(c, pipeName = name)).toList

      // the following hack assumes a single producer per pipe
      // since we don't support multiple producers well when
      // tracking offsets (we'll track offsets for the entire
      // pipe and not per producer
      val producers = conf.getObject("producer")
      val producerName = producers.entrySet().asScala.head.getKey()
      val producerConfig = conf.getConfig(s"producer.${producerName}")
      val producerMappingClasses = if (producerConfig.hasPath("mappings")) producerConfig.getStringList("mappings").asScala else List[String]()
      val producerMappings = producerMappingClasses.map(m ⇒ Class.forName(m).newInstance()).toList.asInstanceOf[List[Mapping]]
      val producerInstance = createProducer(producerName, producerConfig, producerMappings)

      new Pipe(name, consumerInstances, producerInstance)

    } else {
      // disabled
      null
    }
  }).toList.filter(_ != null)

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

  def createConsumer(id: String, pipeName: String): BinlogConsumer = {
    val params = consumers(id)
    val filePos = Conf.binlogFilePos(params.host, params.port, pipeName).getOrElse(BinlogFilePos.current)
    val consumer = BinlogConsumer(params.host, params.port, params.user, params.password, filePos)
    consumer
  }

  def createProducer(id: String, config: Config, mappings: List[Mapping] = List.empty[Mapping]): Producer = {
    try {
      val clazz = producers(id)
      val producer = clazz.getConstructor(classOf[List[Mapping]], classOf[Config]).newInstance(mappings, config)
      producer
    } catch {
      case e: Exception ⇒ {
        Log.severe(s"Failed to configure producer $id: ${e.getMessage}\n${e.getStackTraceString}")
        null
      }
    }
  }
}
